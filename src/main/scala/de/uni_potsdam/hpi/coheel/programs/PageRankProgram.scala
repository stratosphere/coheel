package de.uni_potsdam.hpi.coheel.programs

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._


case class Page(id: Long, rank: Double)
case class Adjacency(id: Long, neighbors: Array[Long])

class PageRankProgram extends NoParamCoheelProgram {

	override def getDescription: String = "PageRank for Articles"

	override def buildProgram(env: ExecutionEnvironment): Unit = {
//		val contextLinks = getContextLinks()
//		val sum = contextLinks.aggregate(Aggregations.SUM, "prob")
//
//		sum.print()

		val NUM_VERTICES = 3
		val NUM_ITERATIONS = 1000
		val THRESHOLD = 0.0001 / NUM_VERTICES
		val INITIAL_RANK = 1.0 / NUM_VERTICES
		val RANDOM_JUMP = 0.05
		val DAMPENING_FACTOR = 1 - RANDOM_JUMP * NUM_VERTICES

		val rawLines : DataSet[String] = env.fromElements(
			"1 2 3",
			"2 1",
			"3 1 2")

		val adjacency = rawLines.map { str =>
			val elements = str.split(' ')
			val id = elements(0).toLong
			val neighbors = elements.slice(1, elements.length).map(_.toLong)
			Adjacency(id, neighbors)
		}

		val initialRanks : DataSet[Page] = adjacency.flatMap { (adj, out: Collector[Page]) =>
			val targets = adj.neighbors
			val rankPerTarget = INITIAL_RANK * DAMPENING_FACTOR / targets.length

			// dampend fraction to targets
			for (target <- targets) {
				out.collect(Page(target, rankPerTarget))
			}

			// random jump to self
			out.collect(Page(adj.id, RANDOM_JUMP));
		}
			.groupBy("id").sum("rank")

		val initialDeltas = initialRanks.map { page =>
			Page(page.id, page.rank - INITIAL_RANK)
		}

		val iteration = initialRanks.iterateDelta(initialDeltas, 100, Array(0)) { (solutionSet, workset) =>
			val deltas = workset.join(adjacency)
				.where("id")
				.equalTo("id")
				.apply { (lastDeltas, adj, out: Collector[Page]) =>
					val targets = adj.neighbors
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

		iteration.print()
	}

}
