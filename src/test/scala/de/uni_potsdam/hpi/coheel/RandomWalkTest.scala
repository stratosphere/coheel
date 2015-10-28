package de.uni_potsdam.hpi.coheelExtractorTest

import de.uni_potsdam.hpi.coheel.programs.ClassificationProgram
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import de.uni_potsdam.hpi.coheel.wiki.{Extractor, WikiPageReader}
import org.jgrapht.graph.{DefaultWeightedEdge, SimpleDirectedWeightedGraph}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._
import scala.io.Source

@RunWith(classOf[JUnitRunner])
class RandomWalkTest extends FunSuite {

	test("RandomWalkNodes are only counted once") {

		val g = new SimpleDirectedWeightedGraph[RandomWalkNode, DefaultWeightedEdge](classOf[DefaultWeightedEdge])

		g.addVertex(RandomWalkNode("a").withNodeType(NodeType.SEED))
		g.addVertex(RandomWalkNode("a").withNodeType(NodeType.CANDIDATE))
		assert(g.vertexSet().size() === 1)
		assert(g.vertexSet().asScala.head.nodeType == NodeType.SEED)
	}


	test("Graph building") {
		// Documentation of the graph see ./doc/random_walk
		val s1 = ClassifierResultWithNeighbours("d1", NodeType.SEED, "s1",
			List(
				Neighbour("c6", 1.0),
				Neighbour("c8", 1.0),
				Neighbour("n4", 1.0),
				Neighbour("n5", 1.0)),
			List(
				Neighbour("s2", 1.0),
				Neighbour("c1", 1.0),
				Neighbour("n1", 1.0),
				Neighbour("n2", 1.0),
				Neighbour("n3", 1.0),
				Neighbour("n4", 1.0))
		)
		val s2 = ClassifierResultWithNeighbours("d1", NodeType.SEED, "s2",
			List(
				Neighbour("s1", 1.0)),
			List(
				Neighbour("c4", 1.0))
		)
		val c1 = ClassifierResultWithNeighbours("d1", NodeType.SEED, "c1",
			List(
				Neighbour("s1", 1.0)),
			List()
		)
		val c2 = ClassifierResultWithNeighbours("d1", NodeType.SEED, "c2",
			List(
				Neighbour("n3", 1.0)),
			List(
				Neighbour("s1", 1.0))
		)
		val c3 = ClassifierResultWithNeighbours("d1", NodeType.SEED, "c3",
			List(
				Neighbour("n3", 1.0)),
			List()
		)
		val c4 = ClassifierResultWithNeighbours("d1", NodeType.SEED, "c4",
			List(
				Neighbour("s3", 1.0)),
			List()
		)
		val c5 = ClassifierResultWithNeighbours("d1", NodeType.SEED, "c5",
			List(
				Neighbour("n6", 1.0)),
			List(
				Neighbour("n6", 1.0))
		)
		val c6 = ClassifierResultWithNeighbours("d1", NodeType.SEED, "c6",
			List(),
			List(
				Neighbour("s1", 1.0))
		)
		val c7 = ClassifierResultWithNeighbours("d1", NodeType.SEED, "c7",
			List(
				Neighbour("n3", 1.0)),
			List(
				Neighbour("c8", 1.0))
		)
		val c8 = ClassifierResultWithNeighbours("d1", NodeType.SEED, "c8",
			List(
				Neighbour("c7", 1.0)),
			List(
				Neighbour("s1", 1.0))
		)


		val classificationProgram = new ClassificationProgram()
		val g = classificationProgram.buildGraph(
			List(
				s1, s2,
				c1, c2, c3, c4, c5, c6, c7, c8))


		val s1Node = RandomWalkNode("s1")
		assert(g.containsVertex(s1Node))
		assert(g.containsEdge(s1Node, s1Node))

	}

}
