package de.uni_potsdam.hpi.coheel.programs

import java.lang.Iterable

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.numerics.abs
import de.uni_potsdam.hpi.coheel.datastructures.TrieHit
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.programs.DataClasses.{ClassifierResultWithNeighbours, NodeTypes, RandomWalkNode}
import de.uni_potsdam.hpi.coheel.util.Timer
import org.apache.commons.collections4.BidiMap
import org.apache.commons.collections4.bidimap.DualHashBidiMap
import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.util.Collector
import org.jgrapht.graph.{DefaultDirectedWeightedGraph, DefaultWeightedEdge}

import scala.collection.JavaConverters._
import scala.collection.mutable

object RandomWalkReduceGroup {
	val STALLING_EDGE_WEIGHT = 0.01f
	val NULL_NODE = "\1"
}

class RandomWalkReduceGroup extends RichGroupReduceFunction[ClassifierResultWithNeighbours, (String, TrieHit, String)] {
	import CoheelLogger._
	import RandomWalkReduceGroup._

	def anyCandidates(entities: Vector[DataClasses.ClassifierResultWithNeighbours]): Boolean = {
		entities.exists { entity => entity.classifierType == NodeTypes.CANDIDATE }
	}

	override def reduce(entitiesIt: Iterable[ClassifierResultWithNeighbours], out: Collector[(String, TrieHit, String)]): Unit = {
		FreeMemory.logMemory(log, "starting RandomWalkReduceGroup")
		// entities contains only SEEDs and CANDIDATEs
		// Note: There is an m:n mapping between candidates and trie hits
		// One trie hit may have many candidate entities (obviously), and also one candidate entity may come from many
		// different trie hit
		val entitiesUnfiltered = entitiesIt.asScala.toVector
		val docId = entitiesUnfiltered.head.documentId
		val sizeBeforePruning = entitiesUnfiltered.size

		var entities = filterUnconnected(entitiesUnfiltered)

		log.info(s"Handling document $docId, entities before pruning: $sizeBeforePruning, entities after pruning: ${entities.size}")
		Timer.start("Document")

		if (entities.isEmpty) {
			logEntities(entitiesUnfiltered)
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

		var i = 0
		var oldEntityMapping: BidiMap[String, Int] = null
		var oldResult: DenseMatrix[Float] = null
		FreeMemory.logMemory(log, "starting random walk loop")

		var initialRandomWalkMatrixSize = -1
		var lastRandomWalkMatrixSize = -1
		var averageMatrixSize = 0
		while (candidatesRemaining) {
			i += 1
			log.trace(s"$i. round: ${entities.count(_.classifierType == NodeTypes.CANDIDATE)} candidates remaining")

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

			Timer.start("buildGraph")
			val g = buildGraph(deduplicatedEntities)
			Timer.logResult(log, "buildGraph")

			Timer.start("buildMatrix")
			val (m, s, entityNodeMapping, candidateIndices) = buildMatrix(g)
			if (initialRandomWalkMatrixSize == -1)
				initialRandomWalkMatrixSize = m.cols
			lastRandomWalkMatrixSize = m.cols
			averageMatrixSize += m.cols
			Timer.logResult(log, "buildMatrix")

			val initial = buildInitialVector(oldEntityMapping, oldResult, entityNodeMapping)

			Timer.start("randomWalk")
			val result = randomWalk(m, s, 6, initial)
			Timer.logResult(log, "randomWalk")

			oldResult = result
			oldEntityMapping = entityNodeMapping

			// find indices of the candidates with their probability
			val candidateIndexProbs = result.toArray.view.zipWithIndex.filter { case (d, idx) => candidateIndices.contains(idx) }
			// it might be, that candidates are not reachable from the seeds, then we abort here
			candidatesRemaining = candidateIndexProbs.nonEmpty
			if (candidatesRemaining) {
				// find best entity
				val (_, maxIdx) = candidateIndexProbs.maxBy(_._1)
				val newEntity = entityNodeMapping.getKey(maxIdx)
//				log.info(s"Found new entity $newEntity")

				// find out all classifiables with this best entity
				val resolvedEntities = entities.filter { ent => ent.candidateEntity == newEntity }
				// set all classifiables with this entity to seed entities
				resolvedEntities.foreach { entity => entity.classifierType = NodeTypes.SEED }
//				log.info(s"Resolved entities: ${resolvedEntities.map(_.shortToString())}")

				// add resolved entites to final alignments
				finalAlignments ++= resolvedEntities

				// determine the newly resolved trie hits
				val resolvedTrieHits = resolvedEntities.map(_.trieHit).toSet
//				log.info(s"Resolved trie hits: $resolvedTrieHits")

				// again, remove all those candidates from the entities, which have trie hits, which we just resolved
				entities = entities.filter { entity => entity.classifierType == NodeTypes.SEED || !resolvedTrieHits.contains(entity.trieHit) }

				candidatesRemaining = anyCandidates(entities)
			}
		}

		val time = Timer.end("Document")
		log.info(s"Finished document $docId with $i random walks in ${time / 1000} s, " +
			s"matrix size - initial: $initialRandomWalkMatrixSize, last: $lastRandomWalkMatrixSize, average: ${averageMatrixSize / i}")
		finalAlignments.map { a => (a.documentId, a.trieHit, a.candidateEntity) }.foreach(out.collect)
	}

	def buildInitialVector(oldEntityMapping: BidiMap[String, Int], oldResult: DenseMatrix[Float], entityNodeMapping: BidiMap[String, Int]): DenseMatrix[Float] = {
		val initial: DenseMatrix[Float] = if (oldEntityMapping != null) {
			var i = 0
			val size = entityNodeMapping.size
			val initialP = new DenseMatrix[Float](1, size)
			while (i < size) {
				val currentEntity = entityNodeMapping.getKey(i)
				val oldIndex = oldEntityMapping.get(currentEntity)
				val oldValue = oldResult(0, oldIndex)
				initialP(0, i) = oldValue
				i += 1
			}
			normalizeToSum1(initialP)
			initialP
		} else
			null
		initial
	}

	def logEntities(entities: Vector[ClassifierResultWithNeighbours]): Unit = {
		if (!CoheelProgram.runsOffline()) {
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
				log.info(s"Entity: ${entity.candidateEntity} (${entity.classifierType}) from '${entity.trieHit}' with ${entity.in.size} in neighbours and ${entity.out.size} out neighbours")
				log.info("In-Neighbours")
				entity.in.foreach { in =>
					log.info(s"I ${in.entity} ${in.prob}")
				}
				log.info("Out-Neighbours")
				entity.out.foreach { out =>
					log.info(s"O ${out.entity} ${out.prob}")
				}
			}
		}
	}

	def filterUnconnected(entities: Vector[ClassifierResultWithNeighbours]): Vector[ClassifierResultWithNeighbours] = {
		val outNeighbours = mutable.Map[String, mutable.Buffer[String]]()
		val visited = mutable.Set[String]()
		val connectedQueue = mutable.Queue[String]()

		// Prepare connected components and initialize out neighbours for all nodes
		entities.foreach { entity =>
			if (entity.classifierType == NodeTypes.SEED) {
				connectedQueue.enqueue(entity.candidateEntity)
				visited += entity.candidateEntity
			}
			entity.in.foreach { in =>
				outNeighbours.get(in.entity) match {
					case Some(x) =>
						x += entity.candidateEntity
					case None =>
						outNeighbours += in.entity -> mutable.ArrayBuffer(entity.candidateEntity)
				}
			}
			outNeighbours += entity.candidateEntity -> entity.out.map(_.entity).toBuffer
		}

		// run connected components
		while (connectedQueue.nonEmpty) {
			val n = connectedQueue.dequeue()
			val outs = outNeighbours.getOrElse(n, List())
			outs.foreach { out =>
				if (!visited.contains(out)) {
					visited += out
					connectedQueue.enqueue(out)
				}
			}
		}

		// Filter all unreachable nodes
		entities.filter { ent =>
			ent.in = ent.in.filter { e => visited.contains(e.entity) }
			ent.out = ent.out.filter { e => visited.contains(e.entity) }
			visited.contains(ent.candidateEntity)
		}
	}

	def buildGraph(entities: Seq[ClassifierResultWithNeighbours]): DefaultDirectedWeightedGraph[RandomWalkNode, DefaultWeightedEdge] = {
		val g = new DefaultDirectedWeightedGraph[RandomWalkNode, DefaultWeightedEdge](classOf[DefaultWeightedEdge])

		val entityMap = mutable.Map[String, RandomWalkNode]()
		val connectedQueue = mutable.Queue[RandomWalkNode]()
//		Timer.start("addSeeds")
		// Make sure candidates and seeds are added first to the graph, so they already exist
		entities.filter(_.classifierType == NodeTypes.SEED).foreach { entity =>
			val node = RandomWalkNode(entity.candidateEntity).withNodeType(NodeTypes.SEED)
			// prepare connected components starting from the seeds
			node.visited = true
			connectedQueue.enqueue(node)
			entityMap.put(entity.candidateEntity, node)
			g.addVertex(node)
		}
//		Timer.logResult(log, "addSeeds")
//		Timer.start("addCandidates")
		entities.filter(_.classifierType == NodeTypes.CANDIDATE).foreach { entity =>
			val node = RandomWalkNode(entity.candidateEntity).withNodeType(NodeTypes.CANDIDATE)
			entityMap.put(entity.candidateEntity, node)
			g.addVertex(node)
		}
//		Timer.logResult(log, "addCandidates")

//		Timer.start("addNeighbours")
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
//		Timer.logResult(log, "addNeighbours")

		val unprunedVertexCount = g.vertexSet().size()
		val unprunedEdgeCount   = g.edgeSet().size


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

//		Timer.start("findUnreachable")
		// remove all the unreachable nodes
		val unreachableNodes = g.vertexSet().asScala.filter(!_.visited)
//		log.info(s"Number of unreachable nodes ${unreachableNodes.size}")
//		Timer.logResult(log, "findUnreachable")
		g.removeAllVertices(unreachableNodes.asJava)

		val inbetweenVertexCount = g.vertexSet().size()
		val inbetweenEdgeCount   = g.edgeSet().size

		// add 0 node
		val nullNode = RandomWalkNode(NULL_NODE).withNodeType(NodeTypes.NULL)
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

		// for neighbour nodes, we do not necessarily have all the outgoing nodes
		// therefore, we need to use the remaining weight by directing it to the null node
		g.vertexSet().asScala.filter(_.nodeType == NodeTypes.NEIGHBOUR).foreach { node =>
			val edgeSum = g.outgoingEdgesOf(node).asScala.toIterator.map { outNode => g.getEdgeWeight(outNode) }.sum
			val e = g.addEdge(node, nullNode)
			g.setEdgeWeight(e, 1.0 - edgeSum)
		}

		// add stalling edges
		g.vertexSet().asScala.foreach { node =>
			val e = g.addEdge(node, node)
			if (e == null) {
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

		val prunedVertexCount = g.vertexSet().size()
		val prunedEdgeCount   = g.edgeSet().size()
		log.trace(s"Unpruned: ($unprunedVertexCount, $unprunedEdgeCount), In-between: ($inbetweenVertexCount, $inbetweenEdgeCount), Pruned: ($prunedVertexCount, $prunedEdgeCount)")
		g
	}

	var data = new Array[Float](0)
	def clearArray(a: Array[Float]): Unit = {
		var i = 0
		while (i < a.length) {
			a(i) = 0
			i += 1
		}
	}
	/**
	 * Builds a random walk matrix from a given graph.
	 *
	 * @return Returns a four tuple of:
	 *         <li>The random walk matrix with each row normalized to 1.0.
	 *         <li>The vector of seed entries with sum 1. If there is a seed at index 2 and 4, and there are 5 entities, this is: [0, 0, 0.5, 0, 0.5]
	 *         <li>The mapping between entity and index in the matrix
	 *         <li>A set of all the indices of the candidates
	 */
	def buildMatrix(g: DefaultDirectedWeightedGraph[RandomWalkNode, DefaultWeightedEdge]): (DenseMatrix[Float], DenseMatrix[Float], BidiMap[String, Int], mutable.Set[Int]) = {
		FreeMemory.logMemory(log, "starting buildMatrix")
		val candidateIndices = mutable.Set[Int]()
		val size = g.vertexSet().size()
		val entityNodeIdMapping = new DualHashBidiMap[String, Int]()

		if (data.length < size * size) {
			log.trace(s"Reinitializing data array from ${data.length} to $size")
			data = new Array[Float](size * size)
		} else {
			clearArray(data)
		}
		val m = new DenseMatrix[Float](size, size, data)

		var currentEntityId = 0
		val s = new DenseMatrix[Float](1, size)
		g.vertexSet().asScala.foreach { node =>
			entityNodeIdMapping.put(node.entity, currentEntityId)
			if (node.nodeType == NodeTypes.CANDIDATE)
				candidateIndices += currentEntityId
			if (node.nodeType == NodeTypes.SEED)
				s(0, currentEntityId) = 1.0f
			currentEntityId += 1
		}

		normalizeToSum1(s)

		g.vertexSet().asScala.foreach { node =>
			val nodeId = entityNodeIdMapping.get(node.entity)
			val outEdges = g.outgoingEdgesOf(node)
			val edgeSum = outEdges.asScala.toList.map(g.getEdgeWeight).sum
			// edgeSum should always sum up to 1.0 + STALLING_EDGE_WEIGHT
			assert(Math.abs(edgeSum - (1.0 + STALLING_EDGE_WEIGHT)) < 0.000001, {
				val outNeighbours = outEdges.asScala.toList.map { e =>
					val target = g.getEdgeTarget(e).entity
					val weight = g.getEdgeWeight(e)
					s"${node.entity} --$weight--> $target"
				}.mkString("\n")

				s"$outNeighbours\nNode $node (${node.nodeType}, ${node.isSink}, ${node.visited}) has outgoing edgeSum = $edgeSum"
			})

			outEdges.asScala.foreach { out =>
				val outTarget = g.getEdgeTarget(out)
				val outWeight = g.getEdgeWeight(out).toFloat
				val outId = entityNodeIdMapping.get(outTarget.entity)
				m(nodeId, outId) = outWeight / (1.0f + STALLING_EDGE_WEIGHT)
			}
		}
		FreeMemory.logMemory(log, "ending buildMatrix")

		(m, s, entityNodeIdMapping, candidateIndices)
	}

	def normalizeToSum1(s: DenseMatrix[Float]): Unit = {
		val size = s.size
		val sum = s.sum
		for (i <- 0 until size)
			s(0, i) = s(0, i) / sum
	}

	val THETA = Math.pow(10, -8).toFloat
	def randomWalk(m: DenseMatrix[Float], s: DenseMatrix[Float], maxIt: Int, startP: DenseMatrix[Float] = null): DenseMatrix[Float] = {
		FreeMemory.logMemory(log, "starting randomWalk")
		var p = if (startP != null) startP else s
		val alpha = 0.15f
		var it = 0
		var diff = 0.0f
		val alphaS = s :* alpha
		m :*= (1 - alpha)
		do {
			val oldP = p
			p = p * m
			p = p + alphaS
			it += 1
			diff = abs(oldP - p).sum
		} while (it < maxIt && diff > THETA)
		log.trace(s"RandomWalk terminating after $it iterations")
		p
	}
}
