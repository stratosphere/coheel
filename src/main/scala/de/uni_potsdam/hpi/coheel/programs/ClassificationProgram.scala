package de.uni_potsdam.hpi.coheel.programs

import java.io.File
import java.lang.Iterable
import java.util.Date

import de.uni_potsdam.hpi.coheel.datastructures.{TrieHit, NewTrie}
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.io.Sample
import de.uni_potsdam.hpi.coheel.ml.CoheelClassifier
import de.uni_potsdam.hpi.coheel.ml.CoheelClassifier.POS_TAG_GROUPS
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import de.uni_potsdam.hpi.coheel.util.{Timer, PerformanceTimer, Util}
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper
import org.apache.commons.collections4.BidiMap
import org.apache.commons.collections4.bidimap.DualHashBidiMap
import org.apache.commons.math3.linear.{RealMatrix, ArrayRealVector, RealVector, OpenMapRealMatrix}
import org.apache.flink.api.common.functions.{Partitioner, RichFlatMapFunction, RichGroupReduceFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.apache.log4j.Logger
import org.jgrapht.graph._
import weka.classifiers.Classifier
import weka.core.SerializationHelper

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.Random


class DocumentPartitioner extends Partitioner[Int] {
	override def partition(index: Int, numPartitions: Int): Int = {
		index
	}
}
class ClassificationProgram extends NoParamCoheelProgram with Serializable {

	override def getDescription: String = "CohEEL Classification"
	def log: Logger = Logger.getLogger(getClass)

	val STALLING_EDGE_WEIGHT = 0.01

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val documents = env.fromElements(Sample.ANGELA_MERKEL_SAMPLE_TEXT_3).name("Documents")


		val tokenizedDocuments = documents.flatMap(new RichFlatMapFunction[String, InputDocument] {
			def log: Logger = Logger.getLogger(getClass)

			var index: Int = -1
			var random: Random = null
			val parallelism = params.parallelism
			log.info(s"Basing distribution on parallelism $parallelism")
			val halfParallelism = if (CoheelProgram.runsOffline()) 1 else parallelism / 2
			val firstHalf  = if (runsOffline()) List(0) else List.range(0, halfParallelism)
			val secondHalf = if (runsOffline()) List(0) else List.range(halfParallelism, parallelism)
			var isFirstHalf: Boolean = true

			override def open(params: Configuration): Unit = {
				index = getRuntimeContext.getIndexOfThisSubtask
				isFirstHalf = firstHalf contains index
				random = new Random()
			}
			override def flatMap(text: String, out: Collector[InputDocument]): Unit = {
				val tokenizer = TokenizerHelper.tokenizeWithPositionInfo(text, null)
				val id = Util.id(text).toString
				log.info(s"Reading document $id on index $index")

				val tokens = tokenizer.getTokens
				val tags = tokenizer.getTags
				if (isFirstHalf) {
					out.collect(InputDocument(id, index, tokens, tags))
					if (!CoheelProgram.runsOffline()) {
						val randomIndex = secondHalf(random.nextInt(halfParallelism))
						out.collect(InputDocument(id, randomIndex, tokens, tags))
						log.info(s"Distributing to $index and $randomIndex")
					}
				} else {
					if (!CoheelProgram.runsOffline()) {
						val randomIndex = firstHalf(random.nextInt(halfParallelism))
						out.collect(InputDocument(id, randomIndex, tokens, tags))
						log.info(s"Distributing to $index and $randomIndex")
					}
					out.collect(InputDocument(id, index, tokens, tags))
				}
			}
		})
		val partitioned = tokenizedDocuments.partitionCustom(new DocumentPartitioner, "index")


		val trieHits = partitioned
			.flatMap(new ClassificationLinkFinderFlatMap(params.parallelism))
			.name("Possible links")

		val features = FeatureProgramHelper.buildFeaturesPerGroup(this, trieHits)
		val basicClassifierResults = features.reduceGroup(new ClassificationFeatureLineReduceGroup).name("Basic Classifier Results")


		val preprocessedNeighbours: DataSet[Neighbours] = loadNeighbours(env)
		val withNeighbours = basicClassifierResults.join(preprocessedNeighbours)
			.where("candidateEntity")
			.equalTo("entity")
			.map { joinResult => joinResult match {
					case (classifierResult, neighbours) =>
						ClassifierResultWithNeighbours(
							classifierResult.documentId,
							classifierResult.classifierType,
							classifierResult.candidateEntity,
							neighbours.in,
							neighbours.out)
				}
			}


		withNeighbours.groupBy("documentId").reduceGroup { entitiesIt =>
			val entities = entitiesIt.toList

			Timer.start("buildGraph")
			val g = buildGraph(entities)
			log.info(s"buildGraph: ${Timer.end("buildGraph")} ms.")

			Timer.start("buildMatrix")
			val (m, s, entityNodeMapping, candidateIndices) = buildMatrix(g)
			log.info(s"buildMatrix: ${Timer.end("buildMatrix")} ms.")

			Timer.start("randomWalk")
			val result = randomWalk(m, s, 100)
			log.info(s"randomWalk: ${Timer.end("randomWalk")} ms.")

			// find candidate with highest probability
			val (maxValue, maxIdx) = result.toArray.zipWithIndex.filter { case (d, idx) => candidateIndices.contains(idx) }.maxBy(_._1)





			(1, 2)
		}.writeAsTsv(randomWalkResultsPath)










		// Write trie hits for debugging
		val trieHitOutput = trieHits.map { trieHit =>
			(trieHit.id, trieHit.surfaceRepr, trieHit.info.trieHit, trieHit.info.posTags.deep)
		}
		trieHitOutput.writeAsTsv(trieHitPath)

		// Write raw features for debugging
		features.reduceGroup { (classifiablesIt, out: Collector[(TrieHit, String, Double, Double)]) =>
			classifiablesIt.foreach { classifiable =>
				import classifiable._
				out.collect((info.trieHit, candidateEntity, surfaceProb, contextProb))
			}
		}.writeAsTsv(rawFeaturesPath)

		// Write candidate classifier results for debugging
		basicClassifierResults.map { res =>
			(res.documentId, res.classifierType, res.candidateEntity, res.trieHit)
		}.writeAsTsv(classificationPath)

	}

	def buildGraph(entities: List[ClassifierResultWithNeighbours]): DefaultDirectedWeightedGraph[RandomWalkNode, DefaultWeightedEdge] = {
		val g = new DefaultDirectedWeightedGraph[RandomWalkNode, DefaultWeightedEdge](classOf[DefaultWeightedEdge])

		val entityMap = mutable.Map[String, RandomWalkNode]()
		val connectedQueue = mutable.Queue[RandomWalkNode]()
		// Make sure candidates and seeds are added first to the graph, so they already exist
		entities.foreach { candidate =>
			val node = RandomWalkNode(candidate.candidateEntity).withNodeType(candidate.classifierType)
			if (node.nodeType == NodeTypes.SEED) {
				node.visited = true
				connectedQueue.enqueue(node)
			}
			entityMap.put(candidate.candidateEntity, node)
			assert(g.addVertex(node))
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
			if (node == nullNode)
				g.setEdgeWeight(e, 1.00)
			else
				g.setEdgeWeight(e, STALLING_EDGE_WEIGHT)
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
		do {
			oldP = p
			p = m.scalarMultiply(1 - alpha).preMultiply(p).add(s.mapMultiply(alpha))
			it += 1
		} while (it < maxIt || oldP.add(p.mapMultiply(-1)).getNorm < THETA)
		p
	}

	def loadNeighbours(env: ExecutionEnvironment): DataSet[Neighbours] = {
		val contextLinks = env.readTextFile(contextLinkProbsPath).map { line =>
			val split = line.split('\t')
			ContextLink(split(0), split(1), split(2).toDouble)
		}
		val outgoingNeighbours = contextLinks.groupBy("from").reduceGroup { grouped =>
			val asList = grouped.toList
			(asList.head.from, asList.map { contextLink => Neighbour(contextLink.to, contextLink.prob) })
		}
		val incomingNeighbours = contextLinks.groupBy("to").reduceGroup { grouped =>
			val asList = grouped.toList
			(asList.head.to, asList.map { contextLink => Neighbour(contextLink.from, contextLink.prob) })
		}
		val preprocessedNeighbours = outgoingNeighbours.join(incomingNeighbours)
			.where(0)
			.equalTo(0)
			.map { joinResult => joinResult match {
					case (out, in) => Neighbours(out._1, out._2, in._2)
				}

		}
		preprocessedNeighbours
	}
}

class ClassificationLinkFinderFlatMap(parallelism: Int) extends RichFlatMapFunction[InputDocument, Classifiable[ClassificationInfo]] {
	var tokenHitCount: Int = 1

	def log = Logger.getLogger(getClass)
	var trie: NewTrie = _
	var fileName: String = _

	override def open(params: Configuration): Unit = {
		val surfacesFile = if (CoheelProgram.runsOffline()) {
//			new File("output/surface-probs.wiki")
			new File("cluster-output/678910")
		} else {
			if (getRuntimeContext.getIndexOfThisSubtask < parallelism / 2)
				new File("/home/hadoop10/data/coheel/12345")
			else
				new File("/home/hadoop10/data/coheel/678910")
		}
		assert(surfacesFile.exists())
		val surfaces = Source.fromFile(surfacesFile, "UTF-8").getLines().flatMap { line =>
			CoheelProgram.parseSurfaceProbsLine(line)
		}
		log.info(s"On subtask id #${getRuntimeContext.getIndexOfThisSubtask} with file ${surfacesFile.getName}")
		log.info(s"Building trie with ${FreeMemory.get(true)} MB")
		val d1 = new Date
		trie = new NewTrie
		surfaces.foreach { surface =>
			trie.add(surface)
		}
		log.info(s"Finished trie with ${FreeMemory.get(true)} MB in ${(new Date().getTime - d1.getTime) / 1000} s")
	}

	override def flatMap(document: InputDocument, out: Collector[Classifiable[ClassificationInfo]]): Unit = {
		trie.findAllInWithTrieHit(document.tokens).foreach { trieHit =>
			val contextOption = Util.extractContext(document.tokens, trieHit.startIndex)

			if (contextOption.isEmpty)
				log.error(s"Could not create context for ${document.id}.")

			contextOption.foreach { case context =>
				val tags = document.tags.slice(trieHit.startIndex, trieHit.startIndex + trieHit.length).toArray
				// TH for trie hit
				val id = s"TH-${document.id}-$tokenHitCount"
				out.collect(Classifiable(id, trieHit.s, context.toArray, info = ClassificationInfo(document.id, trieHit, POS_TAG_GROUPS.map { group => if (group.exists(tags.contains(_))) 1.0 else 0.0 })))
				tokenHitCount += 1
			}
		}
	}

	override def close(): Unit = {
		trie = null
	}
}

class ClassificationFeatureLineReduceGroup extends RichGroupReduceFunction[Classifiable[ClassificationInfo], ClassifierResult] {

	def log = Logger.getLogger(getClass)
	var seedClassifier: CoheelClassifier = null
	var candidateClassifier: CoheelClassifier = null

	override def open(params: Configuration): Unit = {
		val seedPath = if (CoheelProgram.runsOffline()) "RandomForest-10FN.model" else "/home/hadoop10/data/coheel/RandomForest-10FN.model"
		val candidatePath = if (CoheelProgram.runsOffline()) "RandomForest-10FP.model" else "/home/hadoop10/data/coheel/RandomForest-10FP.model"

		log.info(s"Loading models with ${FreeMemory.get(true)} MB")

		val d1 = new Date
		seedClassifier      = new CoheelClassifier(SerializationHelper.read(seedPath).asInstanceOf[Classifier])
		candidateClassifier = new CoheelClassifier(SerializationHelper.read(candidatePath).asInstanceOf[Classifier])

		log.info(s"Finished model with ${FreeMemory.get(true)} MB in ${(new Date().getTime - d1.getTime) / 1000} s")
	}

	override def reduce(candidatesIt: Iterable[Classifiable[ClassificationInfo]], out: Collector[ClassifierResult]): Unit = {
		val allCandidates = candidatesIt.asScala.toSeq
		// TODO: Remove assert if performance problem
		assert(allCandidates.map(_.info.trieHit).toSet.size == 1)
		val trieHit = allCandidates.head.info.trieHit

		val features = new mutable.ArrayBuffer[FeatureLine[ClassificationInfo]](allCandidates.size)
		FeatureProgramHelper.applyCoheelFunctions(allCandidates) { featureLine =>
			features.append(featureLine)
		}
		candidateClassifier.classifyResults(features).foreach { result =>
			out.collect(ClassifierResult(result.model.documentId, NodeTypes.CANDIDATE, result.candidateEntity, trieHit))
		}
		seedClassifier.classifyResults(features).foreach { result =>
			out.collect(ClassifierResult(result.model.documentId, NodeTypes.SEED, result.candidateEntity, trieHit))
		}
	}

	override def close(): Unit = {
		seedClassifier = null
		candidateClassifier = null
	}
}
