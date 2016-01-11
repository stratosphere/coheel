package de.uni_potsdam.hpi.coheel.programs

import java.lang.Iterable

import de.uni_potsdam.hpi.coheel.datastructures.TrieHit
import de.uni_potsdam.hpi.coheel.programs.DataClasses.{ClassifierResultWithNeighbours, NodeTypes, RandomWalkNode}
import de.uni_potsdam.hpi.coheel.util.Timer
import org.apache.commons.collections4.BidiMap
import org.apache.commons.collections4.bidimap.DualHashBidiMap
import org.apache.commons.math3.linear.{ArrayRealVector, OpenMapRealMatrix, RealMatrix, RealVector}
import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.util.Collector
import org.jgrapht.graph.{DefaultDirectedWeightedGraph, DefaultWeightedEdge}

import scala.collection.JavaConverters._
import scala.collection.mutable

object RandomWalkReduceGroup {
	val STALLING_EDGE_WEIGHT = 0.01
}

class RandomWalkReduceGroup extends RichGroupReduceFunction[ClassifierResultWithNeighbours, (String, TrieHit, String)] {
	import CoheelLogger._
	import RandomWalkReduceGroup._

	def anyCandidates(entities: Vector[DataClasses.ClassifierResultWithNeighbours]): Boolean = {
		entities.exists { entity => entity.classifierType == NodeTypes.CANDIDATE }
	}

	override def reduce(entitiesIt: Iterable[ClassifierResultWithNeighbours], out: Collector[(String, TrieHit, String)]): Unit = {
		// entities contains only SEEDs and CANDIDATEs
		// Note: There is an m:n mapping between candidates and trie hits
		// One trie hit may have many candidate entities (obviously), and also one candidate entity may come from many
		// different trie hit
		var entities = entitiesIt.asScala.toVector
		log.info(s"Handling document id: ${entities.head.documentId}")

		log.info("BASIC NEIGHBOURS")
		// For printing out the neighbours, it suffices to group by candidate entity, as the entity determines the neighbours.
		entities.groupBy(_.candidateEntity).map { case (entity, classifiables) =>
			classifiables.find(_.classifierType == NodeTypes.SEED) match {
				case Some(classifiable) =>
					classifiable
				case None =>
					classifiables.head
			}
		}.toVector.foreach { entity =>
			log.info("--------------------------------------------------------")
			log.info(s"Entity: ${entity.candidateEntity} (${entity.classifierType}) from '${entity.trieHit.s}' with ${entity.in.size} in neighbours and ${entity.out.size} out neighbours")
			log.info("In-Neighbours")
			entity.in.foreach { in =>
				log.info(s"I ${in.entity} ${in.prob}")
			}
			log.info("Out-Neighbours")
			entity.out.foreach { out =>
				log.info(s"O ${out.entity} ${out.prob}")
			}
		}

		// we start with the seeds as the final alignments, they are certain
		var finalAlignments = entities.filter { entity => entity.classifierType == NodeTypes.SEED }
		val alignedTrieHits = finalAlignments.map(_.trieHit).toSet
		// remove all those candidates, which have trie hits, which are already resolved, i.e. keep an entity if it is a seed
		// or the trie hit has not yet been resolved
		entities = entities.filter { x => x.classifierType == NodeTypes.SEED || !alignedTrieHits.contains(x.trieHit) }

		var candidatesRemaining = anyCandidates(entities)
		if (!candidatesRemaining)
			log.info("No candidates remaining before first round")

		var i = 1
		while (candidatesRemaining) {
			log.info(s"Start $i. round of random walk with seeds: ${entities.filter(_.classifierType == NodeTypes.SEED).map(_.shortToString())}")
			log.info(s"${entities.count(_.classifierType == NodeTypes.CANDIDATE)} candidates remaining") // TODO: Performance
			log.info(s"Current final alignments: ${finalAlignments.map { a => (a.trieHit, a.candidateEntity) }}")
			log.info(s"Current final alignments: ${finalAlignments.map(_.shortToString())}")
			Timer.start("buildGraph")
			// Each entity may occur only once in the graph. As each classifiable has a candidate entity, which may occur
			// more than once alltogether, we need to remove duplicated candidate entities.
			// If there is a seed among those classifiables with the same candidate entity, we choose the seed, as seeds are relevant
			// for the random walk.
			val deduplicatedEntities = entities.groupBy(_.candidateEntity).map { case (entity, classifiables) =>
				classifiables.find(_.classifierType == NodeTypes.SEED) match {
					case Some(classifiable) =>
						classifiable
					case None =>
						classifiables.head
				}
			}.toSeq

			val g = buildGraph(deduplicatedEntities)
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
			// it might be, that candidates are not reachable from the seeds, then we abort here
			candidatesRemaining = candidateIndexProbs.nonEmpty
			if (candidatesRemaining) {
				// find best entity
				val (_, maxIdx) = candidateIndexProbs.maxBy(_._1)
				val newEntity = entityNodeMapping.getKey(maxIdx)
				log.info(s"Found new entity $newEntity")

				// find out all classifiables with this best entity
				val resolvedEntities = entities.filter { ent => ent.candidateEntity == newEntity }
				// set all classifiables with this entity to seed entities
				resolvedEntities.foreach { entity => entity.classifierType = NodeTypes.SEED }
				log.info(s"Resolved entities: ${resolvedEntities.map(_.shortToString())}")
				// add resolved entites to final alignments
				finalAlignments ++= resolvedEntities

				// determine the newly resolved trie hits
				val resolvedTrieHits = resolvedEntities.map(_.trieHit).toSet
				log.info(s"Resolved trie hits: $resolvedTrieHits")

				log.info(s"Entities before: ${entities.map(_.shortToString())}")
				// again, remove all those candidates from the entities, which have trie hits, which we just resolved
				entities = entities.filter { entity => entity.classifierType == NodeTypes.SEED || !resolvedTrieHits.contains(entity.trieHit) }
				log.info(s"Entities after: ${entities.map(_.shortToString())}")

				candidatesRemaining = anyCandidates(entities)
			} else {
				log.info(s"Aborting, because remaining seeds ${entities.map(_.shortToString())} not reachable from seeds")
			}
			Timer.logResult(log, "findHighest")
			i += 1
		}

		finalAlignments.map { a => (a.documentId, a.trieHit, a.candidateEntity) }.foreach(out.collect)


	}

	def buildGraph(entities: Seq[ClassifierResultWithNeighbours]): DefaultDirectedWeightedGraph[RandomWalkNode, DefaultWeightedEdge] = {
		val g = new DefaultDirectedWeightedGraph[RandomWalkNode, DefaultWeightedEdge](classOf[DefaultWeightedEdge])

		val entityMap = mutable.Map[String, RandomWalkNode]()
		val connectedQueue = mutable.Queue[RandomWalkNode]()
		Timer.start("addSeeds")
		// Make sure candidates and seeds are added first to the graph, so they already exist
		entities.filter(_.classifierType == NodeTypes.SEED).foreach { entity =>
			val node = RandomWalkNode(entity.candidateEntity).withNodeType(NodeTypes.SEED)
			// prepare connected components starting from the seeds
			node.visited = true
			connectedQueue.enqueue(node)
			entityMap.put(entity.candidateEntity, node)
			g.addVertex(node)
		}
		Timer.logResult(log, "addSeeds")
		Timer.start("addCandidates")
		entities.filter(_.classifierType == NodeTypes.CANDIDATE).foreach { entity =>
			val node = RandomWalkNode(entity.candidateEntity).withNodeType(NodeTypes.CANDIDATE)
			entityMap.put(entity.candidateEntity, node)
			g.addVertex(node)
		}
		Timer.logResult(log, "addCandidates")

		Timer.start("addNeighbours")
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
		Timer.logResult(log, "addNeighbours")

		val unprunedVertexCount = g.vertexSet().size()
		val unprunedEdgeCount   = g.edgeSet().size

		Timer.start("connectedComponents")
		// run connected components/connectivity algorithm starting from the seeds
		// unreachable nodes can then be removed
		while (connectedQueue.nonEmpty) {
			val n = connectedQueue.dequeue()
			assert(n.visited)
			val outNeighbours = g.outgoingEdgesOf(n)
			if (outNeighbours.isEmpty)
				n.isSink = true
			outNeighbours.asScala.foreach { out =>
				val target = g.getEdgeTarget(out)
				if (!target.visited) {
					target.visited = true
					connectedQueue.enqueue(target)
				}
			}
		}
		Timer.logResult(log, "connectedComponents")

		Timer.start("findUnreachable")
		// remove all the unreachable nodes
		val unreachableNodes = g.vertexSet().asScala.filter(!_.visited)
		Timer.logResult(log, "findUnreachable")
		Timer.start("removeUnreachable")
		g.removeAllVertices(unreachableNodes.asJava)
		Timer.logResult(log, "removeUnreachable")

		// add 0 node
		val nullNode = RandomWalkNode("0").withNodeType(NodeTypes.NULL)
		g.addVertex(nullNode)

		Timer.start("removeNeighbourSinks")
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
		Timer.logResult(log, "removeNeighbourSinks")

		Timer.start("fillNeighbours")
		// for neighbour nodes, we do not necessarily have all the outgoing nodes
		// therefore, we need to use the remaining weight by directing it to the null node
		g.vertexSet().asScala.filter(_.nodeType == NodeTypes.NEIGHBOUR).foreach { node =>
			val edgeSum = g.outgoingEdgesOf(node).asScala.toIterator.map { outNode => g.getEdgeWeight(outNode) }.sum
			val e = g.addEdge(node, nullNode)
			g.setEdgeWeight(e, 1.0 - edgeSum)
		}
		Timer.logResult(log, "fillNeighbours")

		Timer.start("addStallingEdges")
		// add stalling edges
		g.vertexSet().asScala.foreach { node =>
			val e = g.addEdge(node, node)
			if (e == null) {
				log.error(s"$node apparently links to itself?")
				val existingEdge = g.getEdge(node, node)
				g.setEdgeWeight(existingEdge, g.getEdgeWeight(existingEdge) + STALLING_EDGE_WEIGHT)
			} else {
				if (node == nullNode)
					g.setEdgeWeight(e, 1.0 + STALLING_EDGE_WEIGHT)
				else {
					if (node.isSink)
						g.setEdgeWeight(e, 1.0 + STALLING_EDGE_WEIGHT)
					else
						g.setEdgeWeight(e, STALLING_EDGE_WEIGHT)
				}
			}
		}
		Timer.logResult(log, "addStallingEdges")

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
			// edgeSum should always sum up to 1.0 + STALLING_EDGE_WEIGHT
			assert(Math.abs(edgeSum - (1.0 + STALLING_EDGE_WEIGHT)) < 0.0000000001, {
				val outNeighbours = outEdges.asScala.toList.map { e =>
					val target = g.getEdgeTarget(e).entity
					val weight = g.getEdgeWeight(e)
					s"${node.entity} --$weight--> $target"
				}.mkString("\n")

				s"$outNeighbours\nNode $node (${node.nodeType}, ${node.isSink}, ${node.visited}) has outgoing edgeSum = $edgeSum"
			})

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
