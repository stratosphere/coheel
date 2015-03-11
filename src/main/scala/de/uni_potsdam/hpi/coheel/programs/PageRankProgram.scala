package de.uni_potsdam.hpi.coheel.programs

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import de.uni_potsdam.hpi.coheel.io.OutputFiles._


case class Page(id: String, rank: Double)
case class Adjacency(id: String, neighbours: Array[String])

class PageRankProgram extends NoParamCoheelProgram {

	override def getDescription: String = "PageRank for Articles"

	override def buildProgram(env: ExecutionEnvironment): Unit = {

		val NUM_VERTICES = 342561
		val THRESHOLD = 0.0001 / NUM_VERTICES
		val INITIAL_RANK = 1.0 / NUM_VERTICES

		val NUM_ITERATIONS = 100
		val DAMPENING_FACTOR = 0.85
		val RANDOM_JUMP = (1.0 - DAMPENING_FACTOR) / NUM_VERTICES

		val contextLinks = getContextLinks()

//		contextLinks.aggregate(Aggregations.SUM, "prob").print()
//		contextLinks.groupBy("to").reduceGroup { _ => (1, 1) }.aggregate(Aggregations.SUM, 0).print()

		val adjacency = contextLinks
			.groupBy("to")
			.reduceGroup { contextLinks =>
				val contextLinkArray = contextLinks.toArray
				Adjacency(contextLinkArray.head.from, contextLinkArray.map(_.to))
			}

		val initialRanks = adjacency.flatMap { (adj, out: Collector[Page]) =>
			val targets = adj.neighbours
			val rankPerTarget = INITIAL_RANK * DAMPENING_FACTOR / targets.length

			// dampend fraction to targets
			for (target <- targets) {
				out.collect(Page(target, rankPerTarget))
			}

			// random jump to self
			out.collect(Page(adj.id, RANDOM_JUMP));
		}
			.groupBy("id").sum("rank")

		initialRanks.aggregate(Aggregations.SUM, "rank").print()

		val initialDeltas = initialRanks.map { page =>
			Page(page.id, page.rank - INITIAL_RANK)
		}

		val pageRank = initialRanks.iterateDelta(initialDeltas, NUM_ITERATIONS, Array("id")) { (solutionSet, workset) =>
			val deltas = workset.join(adjacency)
				.where("id")
				.equalTo("id")
				.apply { (lastDeltas, adj, out: Collector[Page]) =>
					val targets = adj.neighbours
					val deltaPerTarget =
						DAMPENING_FACTOR * lastDeltas.rank / targets.length

					for (target <- targets) {
						out.collect(Page(target, deltaPerTarget))
					}
				}
				.groupBy("id").sum("rank")
				.filter { page => Math.abs(page.rank) > THRESHOLD }

			val rankUpdates = solutionSet.join(deltas)
				.where("id")
				.equalTo("id")
				.apply { (current, delta) =>
					Page(current.id, current.rank + delta.rank)
				}


			(rankUpdates, deltas)
		}


		pageRank.writeAsTsv(pageRankPath)

		pageRank.aggregate(Aggregations.SUM, "rank").print()
	}

}
