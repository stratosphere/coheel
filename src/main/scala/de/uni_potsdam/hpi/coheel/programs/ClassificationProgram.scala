package de.uni_potsdam.hpi.coheel.programs

import java.awt.Dimension
import java.io.File
import java.lang.Iterable
import java.util.Date
import javax.swing.{JApplet, JFrame}

import com.mxgraph.layout.mxCircleLayout
import com.mxgraph.swing.mxGraphComponent
import de.uni_potsdam.hpi.coheel.datastructures.NewTrie
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.io.Sample
import de.uni_potsdam.hpi.coheel.ml.CoheelClassifier
import de.uni_potsdam.hpi.coheel.ml.CoheelClassifier.POS_TAG_GROUPS
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import de.uni_potsdam.hpi.coheel.util.Util
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper
import org.apache.commons.collections.bidimap.DualHashBidiMap
import org.apache.commons.collections4.BidiMap
import org.apache.commons.math3.linear.OpenMapRealMatrix
import org.apache.flink.api.common.functions.{Partitioner, RichFlatMapFunction, RichGroupReduceFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.apache.log4j.Logger
import org.jgrapht.{WeightedGraph, ListenableGraph}
import org.jgrapht.ext.JGraphXAdapter
import org.jgrapht.graph._
import weka.classifiers.Classifier
import weka.core.SerializationHelper

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random


class DocumentPartitioner extends Partitioner[Int] {
	override def partition(index: Int, numPartitions: Int): Int = {
		index
	}
}
class ClassificationProgram extends NoParamCoheelProgram {

	override def getDescription: String = "CohEEL Classification"
	def log: Logger = Logger.getLogger(getClass)

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
					val randomIndex = secondHalf(random.nextInt(halfParallelism))
					out.collect(InputDocument(id, randomIndex, tokens, tags))

					log.info(s"Distributing to $index and $randomIndex")
				} else {
					val randomIndex = firstHalf(random.nextInt(halfParallelism))
					out.collect(InputDocument(id, randomIndex, tokens, tags))
					out.collect(InputDocument(id, index, tokens, tags))

					log.info(s"Distributing to $index and $randomIndex")
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
			.map { joinResult =>
				joinResult match {
					case (classifierResult, neighbours) =>
						ClassifierResultWithNeighbours(
							classifierResult.documentId,
							classifierResult.classifierType,
							classifierResult.candidateEntity,
							neighbours.in,
							neighbours.out)
				}
			}


		withNeighbours.groupBy("documentId").reduceGroup { candidatesIt =>
			val candidates = candidatesIt.toList
			val g = buildGraph(candidates)


			val m = new OpenMapRealMatrix(1, 1)


			val entityNodeIdMapping = new DualHashBidiMap()


			(1, 2)
		}.writeAsTsv(randomWalkResultsPath)










		// Write trie hits for debugging
		val trieHitOutput = trieHits.map { trieHit =>
			(trieHit.id, trieHit.surfaceRepr, trieHit.info.trieHit, trieHit.info.posTags.deep)
		}
		trieHitOutput.writeAsTsv(trieHitPath)

		// Write raw features for debugging
		features.reduceGroup { (classifiablesIt, out: Collector[(String, String, Double, Double, Int, Int)]) =>
			classifiablesIt.foreach { classifiable =>
				import classifiable._
				out.collect((surfaceRepr, candidateEntity, surfaceProb, contextProb, info.trieHit.startIndex, info.trieHit.length))
			}
		}.writeAsTsv(rawFeaturesPath)

		// Write candidate classifier results for debugging
		basicClassifierResults.writeAsTsv(classificationPath)

	}

	def buildGraph(candidatesAndSeeds: List[ClassifierResultWithNeighbours]): DefaultDirectedWeightedGraph[RandomWalkNode, DefaultWeightedEdge] = {
		val g = new DefaultDirectedWeightedGraph[RandomWalkNode, DefaultWeightedEdge](classOf[DefaultWeightedEdge])

		val entityMap = mutable.Map[String, RandomWalkNode]()
		val connectedQueue = mutable.Queue[RandomWalkNode]()
		// Make sure candidates and seeds are added first to the graph, so they already exist
		candidatesAndSeeds.foreach { candidate =>
			val node = RandomWalkNode(candidate.candidateEntity).withNodeType(candidate.classifierType)
			if (node.nodeType == NodeType.SEED) {
				node.visited = true
				connectedQueue.enqueue(node)
			}
			entityMap.put(candidate.candidateEntity, node)
			assert(g.addVertex(node))
		}

		// Now also add the neighbours, hopefully also connecting existing seeds and neighbours
		candidatesAndSeeds.foreach { candidate =>
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
		val nullNode = RandomWalkNode("0").withNodeType(NodeType.NULL)
		g.addVertex(nullNode)

		// remove all neighbour sinks, and create corresponding links to the null node
		val neighbourSinks = g.vertexSet().asScala.filter { n => n.isSink && n.nodeType == NodeType.NEIGHBOUR }
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

		g.vertexSet().asScala.filter(_.nodeType == NodeType.NEIGHBOUR).foreach { node =>
			val edgeSum = g.outgoingEdgesOf(node).asScala.map { outNode => g.getEdgeWeight(outNode)}.sum
			val e = g.addEdge(node, nullNode)
			g.setEdgeWeight(e, 1.0 - edgeSum)
		}

		// add stalling edges
		g.vertexSet().asScala.foreach { node =>
			val e = g.addEdge(node, node)
			// TODO: Think about default value
			if (node == nullNode)
				g.setEdgeWeight(e, 1.00)
			else
				g.setEdgeWeight(e, 0.01)
		}

		val prunedVertexCount = g.vertexSet().size()
		val prunedEdgeCount   = g.edgeSet().size()
		log.info(s"Unpruned Graph: ($unprunedVertexCount, $unprunedEdgeCount), Pruned Graph: ($prunedVertexCount, $prunedEdgeCount)")
		g
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
			.map { joinResult =>
				joinResult match {
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
		val features = new mutable.ArrayBuffer[FeatureLine[ClassificationInfo]](allCandidates.size)
		FeatureProgramHelper.applyCoheelFunctions(allCandidates) { featureLine =>
			features.append(featureLine)
		}
		candidateClassifier.classifyResults(features).foreach { result =>
			out.collect(ClassifierResult(result.model.documentId, NodeType.CANDIDATE, result.candidateEntity))
		}
		seedClassifier.classifyResults(features).foreach { result =>
			out.collect(ClassifierResult(result.model.documentId, NodeType.SEED, result.candidateEntity))
		}
	}

	override def close(): Unit = {
		seedClassifier = null
		candidateClassifier = null
	}
}
