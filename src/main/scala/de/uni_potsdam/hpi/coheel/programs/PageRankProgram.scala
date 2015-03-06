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

		val initialRanks : DataSet[Page] = adjacency.map { adj => Page(adj.id, 1.0 / NUM_VERTICES) }

		val iteration = initialRanks.iterate(NUM_ITERATIONS) { pages =>
			val rankContributions = pages.join(adjacency)
				.where("id")
				.equalTo("id") { (page, adj, out: Collector[Page]) => {
					val rankPerTarget = DAMPENING_FACTOR * page.rank / adj.neighbors.length

					// send random jump to self
					out.collect(Page(page.id, RANDOM_JUMP))

					// partial rank to each neighbor
					for (neighbor <- adj.neighbors) {
						out.collect(Page(neighbor, rankPerTarget))
					}
				}
			}
			rankContributions
				.groupBy("id")
				.reduce((a, b) => Page(a.id, a.rank + b.rank))
		}

		iteration.print()
	}

}
