package de.uni_potsdam.hpi.coheel.programs

import java.io.File
import java.lang.Iterable
import java.util.Date

import de.uni_potsdam.hpi.coheel.Params
import de.uni_potsdam.hpi.coheel.datastructures.{TrieHit, NewTrie}
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.io.Sample
import de.uni_potsdam.hpi.coheel.ml.CoheelClassifier
import de.uni_potsdam.hpi.coheel.ml.CoheelClassifier.POS_TAG_GROUPS
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import de.uni_potsdam.hpi.coheel.util.{Timer, Util}
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
import scala.tools.nsc.io.Jar.WManifest.unenrichManifest
import scala.util.Random


class DocumentPartitioner extends Partitioner[Int] {
	override def partition(index: Int, numPartitions: Int): Int = {
		index
	}
}
class ClassificationProgram extends NoParamCoheelProgram with Serializable {

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
			.flatMap(new ClassificationLinkFinderFlatMap(params))
			.name("Possible links")

		val features = FeatureProgramHelper.buildFeaturesPerGroup(this, trieHits)
		val basicClassifierResults = features.reduceGroup(new ClassificationFeatureLineReduceGroup(params)).name("Basic Classifier Results")


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
							classifierResult.trieHit,
							neighbours.in,
							neighbours.out)
				}
			}


		withNeighbours.groupBy("documentId").reduceGroup(new RandomWalkReduceGroup).writeAsTsv(randomWalkResultsPath)

		/*
		 * OUTPUT
		 */
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

class ClassificationLinkFinderFlatMap(params: Params) extends RichFlatMapFunction[InputDocument, Classifiable[ClassificationInfo]] {
	var tokenHitCount: Int = 1

	def log = Logger.getLogger(getClass)
	var trie: NewTrie = _
	var fileName: String = _

	override def open(conf: Configuration): Unit = {
		val surfacesFile = if (CoheelProgram.runsOffline()) {
//			new File("output/surface-probs.wiki")
			new File("cluster-output/678910")
		} else {
			if (getRuntimeContext.getIndexOfThisSubtask < params.parallelism / 2)
				new File(params.config.getString("first_trie_half"))
			else
				new File(params.config.getString("second_trie_half"))
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

class ClassificationFeatureLineReduceGroup(params: Params) extends RichGroupReduceFunction[Classifiable[ClassificationInfo], ClassifierResult] {

	def log = Logger.getLogger(getClass)
	var seedClassifier: CoheelClassifier = null
	var candidateClassifier: CoheelClassifier = null

	override def open(conf: Configuration): Unit = {
		val seedPath = if (CoheelProgram.runsOffline()) "RandomForest-10FN.model" else params.config.getString("seed_model")
		val candidatePath = if (CoheelProgram.runsOffline()) "RandomForest-10FP.model" else params.config.getString("candidate_model")

		log.info(s"Loading models with ${FreeMemory.get(true)} MB")

		val d1 = new Date
		seedClassifier      = new CoheelClassifier(SerializationHelper.read(seedPath).asInstanceOf[Classifier])
		candidateClassifier = new CoheelClassifier(SerializationHelper.read(candidatePath).asInstanceOf[Classifier])

		log.info(s"Finished model with ${FreeMemory.get(true)} MB in ${(new Date().getTime - d1.getTime) / 1000} s")
	}

	override def reduce(candidatesIt: Iterable[Classifiable[ClassificationInfo]], out: Collector[ClassifierResult]): Unit = {
		val allCandidates = candidatesIt.asScala.toSeq
		// TODO: Remove assert if performance problem
		// Assertion: All candidates should come from the same trie hit
		// assert(allCandidates.groupBy { th => (th.info.trieHit.startIndex, th.info.trieHit.length) }.size == 1)
		if (allCandidates.groupBy { th => (th.info.trieHit.startIndex, th.info.trieHit.length) }.size != 1) {
			log.error("More than one trie hit for feature line reducer")
			log.error(allCandidates)
			log.error(allCandidates.groupBy { th => (th.info.trieHit.startIndex, th.info.trieHit.length) })
		}

		val trieHit = allCandidates.head.info.trieHit

		val features = new mutable.ArrayBuffer[FeatureLine[ClassificationInfo]](allCandidates.size)
		FeatureProgramHelper.applyCoheelFunctions(allCandidates) { featureLine =>
			features.append(featureLine)
		}
		var seedsFound = 0
		seedClassifier.classifyResults(features).foreach { result =>
			seedsFound += 1
			out.collect(ClassifierResult(result.model.documentId, NodeTypes.SEED, result.candidateEntity, trieHit))
		}
		log.info(s"Found $seedsFound seeds")
		// only emit candidates, if no seeds were found
		if (seedsFound > 0) {
			candidateClassifier.classifyResults(features).foreach { result =>
				out.collect(ClassifierResult(result.model.documentId, NodeTypes.CANDIDATE, result.candidateEntity, trieHit))
			}
		}
	}

	override def close(): Unit = {
		seedClassifier = null
		candidateClassifier = null
	}
}
