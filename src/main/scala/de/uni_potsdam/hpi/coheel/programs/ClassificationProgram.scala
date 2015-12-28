package de.uni_potsdam.hpi.coheel.programs

import java.io.File
import java.lang.Iterable
import java.util.Date

import de.uni_potsdam.hpi.coheel.Params
import de.uni_potsdam.hpi.coheel.datastructures.{NewTrie, TrieHit}
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.io.Sample
import de.uni_potsdam.hpi.coheel.ml.CoheelClassifier
import de.uni_potsdam.hpi.coheel.ml.CoheelClassifier.POS_TAG_GROUPS
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import de.uni_potsdam.hpi.coheel.util.Util
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper
import org.apache.flink.api.common.functions.{Partitioner, RichFlatMapFunction, RichGroupReduceFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector
import org.apache.log4j.Logger
import weka.classifiers.Classifier
import weka.classifiers.meta.SerialVersionAccess
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


	// Select, which neighbours file to use
	val NEIGHBOURS_FILE: Option[String] = None
//	val NEIGHBOURS_FILE: Option[String] = Some(neighboursPath)

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val documentStrings = (1 to 6).map { x =>
			Source.fromFile(s"src/main/resources/classification-documents/$x", "UTF-8").mkString
		}
//		val documents = env.fromElements(Sample.ANGELA_MERKEL_SAMPLE_TEXT_3).name("Documents")
		val documents = env.fromCollection(documentStrings).name("Documents")

		val inputDocuments = documents.flatMap(new RichFlatMapFunction[String, InputDocument] {
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
				val id = Util.id(text)
				log.info(s"Reading document $id on index $index")

				val tokens = tokenizer.getTokens
				val tags = tokenizer.getTags
				if (isFirstHalf) {
					out.collect(InputDocument(id, 0, index, tokens, tags))
					if (!CoheelProgram.runsOffline()) {
						val randomIndex = secondHalf(random.nextInt(halfParallelism))
						out.collect(InputDocument(id, 1, randomIndex, tokens, tags))
						log.info(s"Distributing to $index and $randomIndex")
					}
				} else {
					if (!CoheelProgram.runsOffline()) {
						val randomIndex = firstHalf(random.nextInt(halfParallelism))
						out.collect(InputDocument(id, 0, randomIndex, tokens, tags))
						log.info(s"Distributing to $index and $randomIndex")
					}
					out.collect(InputDocument(id, 1, index, tokens, tags))
				}
			}
		}).name("Input-Documents")

		val partitionedDocuments = inputDocuments.partitionCustom(new DocumentPartitioner, "index").name("Partitioned-Documents")

		val classifiables = partitionedDocuments
			.flatMap(new PotentialEntityFinderFlatMap(params))
			.name("Possible links")

		// fill the classifiables with all feature information
		val featuresPerGroup = FeatureProgramHelper.buildFeaturesPerGroup(this, classifiables)
		val basicClassifierResults = featuresPerGroup.reduceGroup(new ClassificationReduceGroup(params)).name("Basic Classifier Results")


		val preprocessedNeighbours = NEIGHBOURS_FILE match {
			case Some(file) => loadNeighboursFromHdfs(env, file)
			case None => buildNeighbours(env)
		}

		val withNeighbours = basicClassifierResults.join(preprocessedNeighbours)
			.where("candidateEntity")
			.equalTo("entity")
			.name("Join With Neighbours")
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

		withNeighbours.groupBy("documentId").reduceGroup(new RandomWalkReduceGroup).name("Random Walk").writeAsTsv(randomWalkResultsPath)

		/*
		 * OUTPUT
		 */
		inputDocuments.filter(_.replication == 0).writeAsTsv(inputDocumentsPath)

		if (NEIGHBOURS_FILE.isEmpty) {
			preprocessedNeighbours.map { neighbours =>
				val inString = neighbours.in.map { n => s"${n.entity}\0${n.prob}" }.mkString("\0")
				val outString = neighbours.out.map { n => s"${n.entity}\0${n.prob}" }.mkString("\0")
				s"${neighbours.entity}\t$inString\t$outString"
			}.writeAsText(neighboursPath, FileSystem.WriteMode.OVERWRITE)
		}

		// Write trie hits for debugging
		val trieHitOutput = classifiables.map { trieHit =>
			(trieHit.id, trieHit.surfaceRepr, trieHit.info.trieHit, trieHit.info.posTags.deep, s">>>${trieHit.context.mkString(" ")}<<<")
		}
		trieHitOutput.writeAsTsv(trieHitPath)

		// Write raw features for debugging
		featuresPerGroup.reduceGroup { (classifiablesIt, out: Collector[(TrieHit, String, Double, Double)]) =>
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


	def buildNeighbours(env: ExecutionEnvironment): DataSet[Neighbours] = {
		val contextLinks = env.readTextFile(contextLinkProbsPath).name("ContextLinkProbs-Path").map { line =>
			val split = line.split('\t')
			ContextLink(split(0), split(1), split(2).toDouble)
		}.name("ContextLinks")
		val outgoingNeighbours = contextLinks.groupBy("from").reduceGroup { grouped =>
			val asList = grouped.toList
			(asList.head.from, asList.map { contextLink => Neighbour(contextLink.to, contextLink.prob) })
		}.name("Outgoing Neighbours")
		val incomingNeighbours = contextLinks.groupBy("to").reduceGroup { grouped =>
			val asList = grouped.toList
			(asList.head.to, asList.map { contextLink => Neighbour(contextLink.from, contextLink.prob) })
		}.name("Incoming Neighbours")
		val preprocessedNeighbours = incomingNeighbours.join(outgoingNeighbours)
			.where(0)
			.equalTo(0)
			.map { joinResult => joinResult match {
					case (in, out) => Neighbours(in._1, in._2, out._2)
				}
		}.name("All-Neighbours")
		preprocessedNeighbours
	}

	def loadNeighboursFromHdfs(env: ExecutionEnvironment, neighboursPath: String): DataSet[Neighbours] = {
		env.readTextFile(neighboursPath).map { neighboursLine =>
			val Array(entity, inString, outString) = neighboursLine.split('\t')
			val inNeighbours = inString.split("\0").grouped(2).map { case Array(ent, prob) => Neighbour(ent, prob.toDouble) }.toSeq
			val outNeighbours = outString.split("\0").grouped(2).map { case Array(ent, prob) => Neighbour(ent, prob.toDouble) }.toSeq
			Neighbours(entity, inNeighbours, outNeighbours)
		}.name("All-Neighbours")
	}
}

class PotentialEntityFinderFlatMap(params: Params) extends RichFlatMapFunction[InputDocument, Classifiable[ClassificationInfo]] {
	var tokenHitCount: Int = 1

	def log = Logger.getLogger(getClass)
	var trie: NewTrie = _
	var fileName: String = _

	override def open(conf: Configuration): Unit = {
		val surfaceLinkProbsFile = if (CoheelProgram.runsOffline()) {
			new File("output/surface-link-probs.wiki")
//			new File("cluster-output/678910")
		} else {
			if (getRuntimeContext.getIndexOfThisSubtask < params.parallelism / 2)
				new File(params.config.getString("first_trie_half"))
			else
				new File(params.config.getString("second_trie_half"))
		}
		val surfaces = Source.fromFile(surfaceLinkProbsFile, "UTF-8").getLines().flatMap { line =>
			val split = line.split('\t')
			if (split.length == 5)
				Some(split(0), split(3).toFloat)
			else {
				log.warn(s"SurfaceLinkProbs: Discarding '${split.deep}' because split size not correct")
				log.warn(line)
				None
			}
		}
		log.info(s"On subtask id #${getRuntimeContext.getIndexOfThisSubtask} with file ${surfaceLinkProbsFile.getName}")
		log.info(s"Building trie with ${FreeMemory.get(true)} MB")
		val d1 = new Date
		trie = new NewTrie
		surfaces.foreach { case (surface, prob) =>
			// TODO: Determine heuristic for this value
			if (prob > 0.05) {
//				println(s"Adding $surface with prob $prob")
				trie.add(surface, prob)
			}
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
				val containsNoun = tags.exists { t => t.startsWith("N")}
				// TH for trie hit
				if (containsNoun) {
					val id = s"TH-${document.id}-${document.replication}-$tokenHitCount"
					out.collect(Classifiable(id, trieHit.s, context.toArray, info = ClassificationInfo(document.id, trieHit, POS_TAG_GROUPS.map { group => if (group.exists(tags.contains(_))) 1.0 else 0.0 })))
					tokenHitCount += 1
				}
				else {
					log.warn(s"Removing because no noun: $trieHit in >>${context.slice(20, 30).mkString(" ")}<<")
				}
			}
		}
	}

	override def close(): Unit = {
		trie = null
	}
}

class ClassificationReduceGroup(params: Params) extends RichGroupReduceFunction[Classifiable[ClassificationInfo], ClassifierResult] {

	def log = Logger.getLogger(getClass)
	var seedClassifier: CoheelClassifier = null
	var candidateClassifier: CoheelClassifier = null

	override def open(conf: Configuration): Unit = {
		val seedPath      = if (CoheelProgram.runsOffline()) "RandomForest-SEED-and-CANDIDATE.model" else params.config.getString("seed_model")
		log.info(s"Seed path is $seedPath")
		val candidatePath = if (CoheelProgram.runsOffline()) "RandomForest-SEED-and-CANDIDATE.model" else params.config.getString("candidate_model")
		log.info(s"Candidate path is $candidatePath")
		log.info(s"Loading models with ${FreeMemory.get(true)} MB")

		val start = new Date
		seedClassifier      = new CoheelClassifier(SerializationHelper.read(seedPath).asInstanceOf[Classifier])
//		candidateClassifier = new CoheelClassifier(SerializationHelper.read(candidatePath).asInstanceOf[Classifier])
		candidateClassifier = seedClassifier

		log.info(s"Finished model loading with ${FreeMemory.get(true)} MB in ${(new Date().getTime - start.getTime) / 1000} s")
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
		seedClassifier.classifyResultsWithSeedLogic(features).foreach { result =>
			seedsFound += 1
			out.collect(ClassifierResult(result.info.documentId, NodeTypes.SEED, result.candidateEntity, trieHit))
		}
		log.info(s"Classification for $trieHit")
		log.info("Candidates:")
		allCandidates.foreach { candidate =>
			log.info(f"    ${candidate.candidateEntity}%.30s ${candidate.surfaceProb}%.3f ${candidate.contextProb}%.0f")
		}
		log.info(s"Found #seeds: $seedsFound")
		// only emit candidates, if no seeds were found
		if (seedsFound == 0) {
			var candidatesFound = 0
			candidateClassifier.classifyResultsWithCandidateLogic(features).foreach { result =>
				candidatesFound += 1
				out.collect(ClassifierResult(result.info.documentId, NodeTypes.CANDIDATE, result.candidateEntity, trieHit))
			}
			log.info(s"Found #candidates: $candidatesFound")
		}
	}

	override def close(): Unit = {
		seedClassifier = null
		candidateClassifier = null
	}
}
