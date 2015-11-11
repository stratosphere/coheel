package de.uni_potsdam.hpi.coheel.programs

import java.lang.Iterable

import de.uni_potsdam.hpi.coheel.datastructures.TrieHit
import de.uni_potsdam.hpi.coheel.programs.DataClasses.{RandomWalkNode, NodeTypes, ClassifierResultWithNeighbours}
import org.apache.commons.collections4.BidiMap
import org.apache.commons.collections4.bidimap.DualHashBidiMap
import org.apache.commons.math3.linear.{ArrayRealVector, OpenMapRealMatrix, RealVector, RealMatrix}
import org.apache.log4j.Logger
import de.uni_potsdam.hpi.coheel.util.Timer
import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.util.Collector
import org.jgrapht.graph.{DefaultWeightedEdge, DefaultDirectedWeightedGraph}
import scala.collection.JavaConverters._
import scala.collection.mutable

class RandomWalkReduceGroup extends RichGroupReduceFunction[ClassifierResultWithNeighbours, (String, TrieHit, String)] {

	def log: Logger = Logger.getLogger(getClass)
	val STALLING_EDGE_WEIGHT = 0.01

	override def reduce(entitiesIt: Iterable[ClassifierResultWithNeighbours], out: Collector[(String, TrieHit, String)]): Unit = {
		// entities contains only of SEEDs and CANDIDATEs
		val entities = entitiesIt.asScala.toVector

		log.warn("BASIC NEIGHBOURS")
		entities.foreach { entity =>
			log.warn("--------------------------------------------------------")
			log.warn(s"Entity: ${entity.candidateEntity} (${entity.classifierType}) from '${entity.trieHit.s}' with ${entity.in.size} in neighbours and ${entity.out.size} out neighbours")
			log.warn("In-Neighbours")
			entity.in.foreach { in =>
				log.warn(s"  ${in.entity}")
			}
			println("Out-Neighbours")
			entity.in.foreach { out =>
				log.warn(s"  ${out.entity}")
			}
		}

		// we start with the seeds as the final alignments, they are certain
		var finalAlignments = entities.filter { entity => entity.classifierType == NodeTypes.SEED }

		// get all the candidates
		var candidates = entities.filter { entity => entity.classifierType == NodeTypes.CANDIDATE }
		// filter all those candidates, which link to seed entities
		//			var resolvedCandidates = candidates.filter { candidate => resolvedEntities.contains(candidate.candidateEntity) }
		// add them to fin
		//			finalAlignments ++= resolvedCandidates
		//			candidates = candidates.filter { candidate => !resolvedCandidates.contains(candidate) }

		var candidatesRemaining = candidates.nonEmpty
		if (!candidatesRemaining)
			log.info("No candidates remaining before first round")

		var i = 1
		while (candidatesRemaining) {
			log.info(s"Start $i. round of random walk with seeds: ${entities.filter(_.candidateEntity == NodeTypes.SEED)}")
			log.info(s"${candidates.size} candidates remaining")
			Timer.start("buildGraph")
			val g = buildGraph(entities)
			log.warn("INNER LOOP")
			g.vertexSet().asScala.foreach { e =>
				log.warn(s"Entity: ${e.entity} (${e.nodeType}})")
			}
			log.info(s"Method buildGraph took ${Timer.end("buildGraph")} ms.")

			Timer.start("buildMatrix")
			val (m, s, entityNodeMapping, candidateIndices) = buildMatrix(g)
			log.info(s"Method buildMatrix took ${Timer.end("buildMatrix")} ms.")

			Timer.start("randomWalk")
			val result = randomWalk(m, s, 100)
			log.info(s"Method randomWalk took ${Timer.end("randomWalk")} ms.")

			Timer.start("findHighest")
			// find indices of the candidates with their probability
			val candidateIndexProbs = result.toArray.zipWithIndex.filter { case (d, idx) => candidateIndices.contains(idx) }
			candidatesRemaining = candidateIndexProbs.nonEmpty
			if (candidatesRemaining) {
				val (_, maxIdx) = candidateIndexProbs.maxBy(_._1)
				val newEntity = entityNodeMapping.getKey(maxIdx)
				log.info(s"Found new entity $newEntity")
				log.info()
				val (resolvedCandidates, remainingCandidates) = candidates.partition { can => can.candidateEntity == newEntity }
				finalAlignments ++= resolvedCandidates
				candidates = remainingCandidates
				entities.foreach { entity => if (entity.candidateEntity == newEntity) entity.classifierType = NodeTypes.SEED }

				candidatesRemaining = candidates.nonEmpty
			}
			log.info(s"Method findHighest took ${Timer.end("findHighest")} ms.")
			i += 1
		}

		finalAlignments.map { a => (a.documentId, a.trieHit, a.candidateEntity) }.foreach(out.collect)


	}

	def buildGraph(entities: Vector[ClassifierResultWithNeighbours]): DefaultDirectedWeightedGraph[RandomWalkNode, DefaultWeightedEdge] = {
		val g = new DefaultDirectedWeightedGraph[RandomWalkNode, DefaultWeightedEdge](classOf[DefaultWeightedEdge])

		val entityMap = mutable.Map[String, RandomWalkNode]()
		val connectedQueue = mutable.Queue[RandomWalkNode]()
		// Make sure candidates and seeds are added first to the graph, so they already exist
		entities.filter(_.classifierType == NodeTypes.SEED).foreach { entity =>
			val node = RandomWalkNode(entity.candidateEntity).withNodeType(NodeTypes.SEED)
			// prepare connected components starting from the seeds
			node.visited = true
			connectedQueue.enqueue(node)
			entityMap.put(entity.candidateEntity, node)
			g.addVertex(node)
		}
		entities.filter(_.classifierType == NodeTypes.CANDIDATE).foreach { entity =>
			val node = RandomWalkNode(entity.candidateEntity).withNodeType(NodeTypes.CANDIDATE)
			entityMap.put(entity.candidateEntity, node)
			g.addVertex(node)
		}

		// Now also add the neighbours, hopefully also connecting existing seeds and neighbours
		entities.foreach { candidate =>
			val currentNode = entityMap.get(candidate.candidateEntity).get
			candidate.in.foreach { candidateIn =>
				val inNode = entityMap.get(candidateIn.entity) match {
					case Some(node) =>
						node
					case None =>
						val node = RandomWalkNode(candidateIn.entity)
						entityMap.put(candidateIn.entity, node)
						g.addVertex(node)
						node
				}
				if (g.containsEdge(inNode, currentNode)) {
					val e = g.getEdge(inNode, currentNode)
					assert(g.getEdgeWeight(e) == candidateIn.prob)
				}
				else {
					val e = g.addEdge(inNode, currentNode)
					g.setEdgeWeight(e, candidateIn.prob)
				}
			}
			candidate.out.foreach { candidateOut =>
				val outNode = entityMap.get(candidateOut.entity) match {
					case Some(node) =>
						node
					case None =>
						val node = RandomWalkNode(candidateOut.entity)
						entityMap.put(candidateOut.entity, node)
						g.addVertex(node)
						node
				}
				if (g.containsEdge(currentNode, outNode)) {
					val e = g.getEdge(currentNode, outNode)
					assert(g.getEdgeWeight(e) == candidateOut.prob)
				} else {
					val e = g.addEdge(currentNode, outNode)
					g.setEdgeWeight(e, candidateOut.prob)
				}
			}
		}

		// run connected components/connectivity algorithm starting from the seeds
		// unreachable nodes can then be removed
		while (connectedQueue.nonEmpty) {
			val n = connectedQueue.dequeue()
			//			println(s"${n.entity}")
			val outNeighbours = g.outgoingEdgesOf(n)
			if (outNeighbours.isEmpty)
				n.isSink = true
			outNeighbours.asScala.foreach { out =>
				val target = g.getEdgeTarget(out)
				//				println(s"  -> ${target.entity}")
				if (!target.visited) {
					target.visited = true
					connectedQueue.enqueue(target)
				}
			}
			//			println(s"Neighbours: ${g.vertexSet().asScala.filter(!_.visited).toList.sortBy(_.entity)}")
			//			println()
		}

		val unprunedVertexCount = g.vertexSet().size()
		val unprunedEdgeCount   = g.edgeSet().size

		// remove all the unreachable nodes
		val unreachableNodes = g.vertexSet().asScala.filter(!_.visited)
		g.removeAllVertices(unreachableNodes.asJava)

		// add 0 node
		val nullNode = RandomWalkNode("0").withNodeType(NodeTypes.NULL)
		g.addVertex(nullNode)

		// remove all neighbour sinks, and create corresponding links to the null node
		val neighbourSinks = g.vertexSet().asScala.filter { n => n.isSink && n.nodeType == NodeTypes.NEIGHBOUR }
		neighbourSinks.foreach { node =>
			g.incomingEdgesOf(node).asScala.foreach { in =>
				val inNode = g.getEdgeSource(in)
				val weight = g.getEdgeWeight(in)
				val e = g.addEdge(inNode, nullNode)
				if (e == null) {
					val e = g.getEdge(inNode, nullNode)
					val currentWeight = g.getEdgeWeight(e)
					g.setEdgeWeight(e, currentWeight + weight)
				} else {
					g.setEdgeWeight(e, weight)
				}
			}
		}
		g.removeAllVertices(neighbourSinks.asJava)

		g.vertexSet().asScala.filter(_.nodeType == NodeTypes.NEIGHBOUR).foreach { node =>
			val edgeSum = g.outgoingEdgesOf(node).asScala.map { outNode => g.getEdgeWeight(outNode)}.sum
			val e = g.addEdge(node, nullNode)
			g.setEdgeWeight(e, 1.0 - edgeSum)
		}

		// add stalling edges
		g.vertexSet().asScala.foreach { node =>
			val e = g.addEdge(node, node)
			if (e == null) {
				log.error(s"$node apparently links to itself?")
			} else {
				if (node == nullNode)
					g.setEdgeWeight(e, 1.00)
				else
					g.setEdgeWeight(e, STALLING_EDGE_WEIGHT)
			}
		}

		val prunedVertexCount = g.vertexSet().size()
		val prunedEdgeCount   = g.edgeSet().size()
		log.info(s"Unpruned Graph: ($unprunedVertexCount, $unprunedEdgeCount), Pruned Graph: ($prunedVertexCount, $prunedEdgeCount)")
		g
	}

	/**
	 * Builds a random walk matrix from a given graph.
	 * @return Returns a four tuple of:
	 *         <li>The random walk matrix with each row normalized to 1.0.
	 *         <li>The vector of seed entries with sum 1. If there is a seed at index 2 and 4, and there are 5 entities, this is: [0, 0, 0.5, 0, 0.5]
	 *         <li>The mapping between entity and index in the matrix
	 *         <li>A set of all the indices of the candidates
	 */
	def buildMatrix(g: DefaultDirectedWeightedGraph[RandomWalkNode, DefaultWeightedEdge]): (RealMatrix, RealVector, BidiMap[String, Int], mutable.Set[Int]) = {
		val candidateIndices = mutable.Set[Int]()
		val size = g.vertexSet().size()
		val entityNodeIdMapping = new DualHashBidiMap[String, Int]()
		val m = new OpenMapRealMatrix(size, size)

		var currentEntityId = 0
		val s: RealVector = new ArrayRealVector(size)
		g.vertexSet().asScala.foreach { node =>
			entityNodeIdMapping.put(node.entity, currentEntityId)
			if (node.nodeType == NodeTypes.CANDIDATE)
				candidateIndices += currentEntityId
			if (node.nodeType == NodeTypes.SEED)
				s.setEntry(currentEntityId, 1.0)
			currentEntityId += 1
		}
		val sum = s.getL1Norm
		for (i <- 0 until size)
			s.setEntry(i, s.getEntry(i) / sum)

		g.vertexSet().asScala.foreach { node =>
			val nodeId = entityNodeIdMapping.get(node.entity)
			val outEdges = g.outgoingEdgesOf(node)
			val edgeSum = outEdges.asScala.toList.map(g.getEdgeWeight).sum
			assert(edgeSum - (1.0 + STALLING_EDGE_WEIGHT) < 0.0001)

			outEdges.asScala.foreach { out =>
				val outTarget = g.getEdgeTarget(out)
				val outWeight = g.getEdgeWeight(out)
				val outId = entityNodeIdMapping.get(outTarget.entity)
				m.setEntry(nodeId, outId, outWeight / (1.0 + STALLING_EDGE_WEIGHT))
			}
		}
		(m, s, entityNodeIdMapping, candidateIndices)
	}

	val THETA = Math.pow(10, -8)
	def randomWalk(m: RealMatrix, s: RealVector, maxIt: Int): RealVector = {
		var p = s
		var oldP = s
		val alpha = 0.15
		var it = 0
		var diff = 0.0
		do {
			oldP = p
			p = m.scalarMultiply(1 - alpha).preMultiply(p).add(s.mapMultiply(alpha))
			it += 1
			diff = oldP.add(p.mapMultiply(-1)).getNorm
		} while (it < maxIt && diff > THETA)
		log.info(s"RandomWalk termininating with diff $diff after $it iterations")
		p
	}
}
