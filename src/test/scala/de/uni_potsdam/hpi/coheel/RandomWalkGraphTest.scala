package de.uni_potsdam.hpi.coheel

import de.uni_potsdam.hpi.coheel.programs.ClassificationProgram
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import de.uni_potsdam.hpi.coheel.wiki.{Extractor, WikiPageReader}
import org.jgrapht.graph.{DefaultWeightedEdge, SimpleDirectedWeightedGraph}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class RandomWalkGraphTest extends FunSuite {


	val g = {
		// Documentation of the graph see ./doc/random_walk
		val s1 = ClassifierResultWithNeighbours("d1", NodeType.SEED, "s1",
			List(
				Neighbour("c6", 1.00),
				Neighbour("c8", 1.00),
				Neighbour("n4", 0.05),
				Neighbour("n5", 0.05)),
			List(
				Neighbour("s2", 0.25),
				Neighbour("c1", 0.05),
				Neighbour("n1", 0.10),
				Neighbour("n2", 0.15),
				Neighbour("n3", 0.20),
				Neighbour("n4", 0.25))
		)
		val s2 = ClassifierResultWithNeighbours("d1", NodeType.SEED, "s2",
			List(
				Neighbour("s1", 0.25)),
			List(
				Neighbour("c4", 1.00))
		)
		val c1 = ClassifierResultWithNeighbours("d1", NodeType.CANDIDATE, "c1",
			List(
				Neighbour("s1", 0.05)),
			List()
		)
		val c2 = ClassifierResultWithNeighbours("d1", NodeType.CANDIDATE, "c2",
			List(
				Neighbour("n3", 0.30)),
			List(
				Neighbour("s1", 1.00))
		)
		val c3 = ClassifierResultWithNeighbours("d1", NodeType.CANDIDATE, "c3",
			List(
				Neighbour("n3", 0.20)),
			List()
		)
		val c4 = ClassifierResultWithNeighbours("d1", NodeType.CANDIDATE, "c4",
			List(
				Neighbour("s2", 1.0)),
			List()
		)
		val c5 = ClassifierResultWithNeighbours("d1", NodeType.CANDIDATE, "c5",
			List(
				Neighbour("n6", 0.15)),
			List(
				Neighbour("n6", 1.00))
		)
		val c6 = ClassifierResultWithNeighbours("d1", NodeType.CANDIDATE, "c6",
			List(),
			List(
				Neighbour("s1", 1.00))
		)
		val c7 = ClassifierResultWithNeighbours("d1", NodeType.CANDIDATE, "c7",
			List(
				Neighbour("n3", 0.10)),
			List(
				Neighbour("c8", 0.60),
				Neighbour("n7", 0.40))
		)
		val c8 = ClassifierResultWithNeighbours("d1", NodeType.CANDIDATE, "c8",
			List(
				Neighbour("c7", 0.60)),
			List(
				Neighbour("s1", 1.00))
		)


		val classificationProgram = new ClassificationProgram()
		val g = classificationProgram.buildGraph(
			List(
				s1, s2,
				c1, c2, c3, c4, c5, c6, c7, c8))
		g
	}
	val s1Node = RandomWalkNode("s1")
	val s2Node = RandomWalkNode("s2")
	val c1Node = RandomWalkNode("c1")
	val c3Node = RandomWalkNode("c3")
	val c5Node = RandomWalkNode("c5")
	val c6Node = RandomWalkNode("c6")
	val c7Node = RandomWalkNode("c7")
	val c8Node = RandomWalkNode("c8")
	val n1Node = RandomWalkNode("n1")
	val n2Node = RandomWalkNode("n2")
	val n3Node = RandomWalkNode("n3")
	val n4Node = RandomWalkNode("n4")
	val n5Node = RandomWalkNode("n5")
	val n6Node = RandomWalkNode("n6")
	val n7Node = RandomWalkNode("n7")
	val nullNode  = RandomWalkNode("0")

	test("RandomWalkNodes are only counted once") {
		val g = new SimpleDirectedWeightedGraph[RandomWalkNode, DefaultWeightedEdge](classOf[DefaultWeightedEdge])

		g.addVertex(RandomWalkNode("a").withNodeType(NodeType.SEED))
		g.addVertex(RandomWalkNode("a").withNodeType(NodeType.CANDIDATE))
		assert(g.vertexSet().size() === 1)
		assert(g.vertexSet().asScala.head.nodeType == NodeType.SEED)
	}


	test("Candidate C3 reachable over neighbour N3") {
		assert(g.containsVertex(c3Node))
		assert(g.containsVertex(n3Node))
	}

	test("Unreachable candidate C5 is removed") {
		assert(!g.containsVertex(c5Node))
	}

	test("Ingoing link from neighbour N5 to seed S1 gets removed") {
		assert(!g.containsVertex(n5Node))
		assert(!g.containsEdge(s1Node, n5Node))
	}

	test("Outgoing neighbour sinks N1 and N2 are removed and combined in 0") {
		assert(!g.containsVertex(n1Node))
		assert(!g.containsVertex(n2Node))
		assert(g.containsEdge(s1Node, nullNode))
	}

	test("Weight of removed nodes N1 and N2 is added for S1") {
		val e = g.getEdge(s1Node, nullNode)
		assert(g.getEdgeWeight(e) === 0.25)
	}

	test("Null node only links to itself with 100 % prob") {
		assert(g.outDegreeOf(nullNode) === 1)
		val e = g.getEdge(nullNode, nullNode)
		assert(g.getEdgeWeight(e) === 1.00)
	}

	test("Sink N7 is removed") {
		assert(!g.containsVertex(n7Node))
	}

	test("Removed neighbours are replaced by nullNode edges") {
		val e = g.getEdge(c7Node, nullNode)
		assert(g.getEdgeWeight(e) === 0.40)
	}

	test("Fill remaining space of neighbours with nullNode edges") {
		val e1 = g.getEdge(n4Node, nullNode)
		assert(g.getEdgeWeight(e1) === 0.95)

		val e2 = g.getEdge(n3Node, nullNode)
		assert(g.getEdgeWeight(e2) === 0.40)
	}

	test("Candidate C1 directly reachable over seed S1") {
		assert(g.containsVertex(c1Node))
	}

	test("Ingoing link from candidate C6 to seed S1 gets removed, as C6 not reachable") {
		assert(!g.containsVertex(c6Node))
	}

	test("Ingoing link from candidate C8 gets not removed if reachable by other candidate C7") {
		assert(g.containsVertex(c7Node))
		assert(g.containsVertex(c8Node))
		assert(g.containsEdge(c7Node, c8Node))
		assert(g.containsEdge(c8Node, s1Node))
	}

	test("Contains stalling edges") {
		assert(g.containsEdge(s1Node, s1Node))
		assert(g.containsEdge(c1Node, c1Node))
		assert(g.containsEdge(n3Node, n3Node))
		assert(g.containsEdge(nullNode, nullNode))
	}

}
