package de.uni_potsdam.hpi.coheel.programs

import java.lang.Iterable
import java.net.URI
import java.util.Date

import de.uni_potsdam.hpi.coheel.Params
import de.uni_potsdam.hpi.coheel.datastructures.TrieHit
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.io.Sample
import de.uni_potsdam.hpi.coheel.ml.CoheelClassifier
import de.uni_potsdam.hpi.coheel.ml.CoheelClassifier.POS_TAG_GROUPS
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import de.uni_potsdam.hpi.coheel.util.Util
import org.apache.flink.api.common.functions.{Partitioner, RichGroupReduceFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.{Path, FileSystem}
import org.apache.flink.util.Collector
import weka.classifiers.Classifier
import weka.core.SerializationHelper

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source


class DocumentPartitioner extends Partitioner[Int] {
	override def partition(index: Int, numPartitions: Int): Int = {
		index
	}
}
class ClassificationProgram extends NoParamCoheelProgram with Serializable {

	import CoheelLogger._

	override def getDescription: String = "CohEEL Classification"

	// Select, which neighbours file to use
	val NEIGHBOURS_FILE = fullNeighboursPath
//	val NEIGHBOURS_FILE = reciprocalNeighboursPath

	val neighboursCreationMethod = Map(fullNeighboursPath -> buildFullNeighbours _, reciprocalNeighboursPath -> buildReciprocalNeighbours _)

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val documentStrings = List(4).map { x =>
			Source.fromFile(s"src/main/resources/classification-documents/$x", "UTF-8").mkString
		}
		val documents = if (runsOffline())
				env.fromElements(Sample.ANGELA_MERKEL_SAMPLE_TEXT_3).name("Documents")
			else
//				env.fromElements(Sample.ANGELA_MERKEL_SAMPLE_TEXT_3).name("Documents")
				env.fromCollection(documentStrings).name("Documents")

		val inputDocuments = documents.flatMap(new InputDocumentDistributorFlatMap(params, runsOffline())).name("Input-Documents")

		val partitionedDocuments = inputDocuments.partitionCustom(new DocumentPartitioner, "index").name("Partitioned-Documents")

		val classifiables = partitionedDocuments
			.flatMap(new RunTrieOverDocumentsFlatMap(params))
			.name("Possible links")

		// fill the classifiables with all feature information
		val featuresPerGroup = FeatureHelper.buildFeaturesPerGroup(env, classifiables)
		val basicClassifierResults = featuresPerGroup.reduceGroup(new ClassificationReduceGroup(params)).name("Basic Classifier Results")

		val f = FileSystem.get(new URI("hdfs://tenemhead2"))
		val preprocessedNeighbours = if (f.exists(new Path(NEIGHBOURS_FILE.replace("hdfs://tenemhead2", "")))) {
			log.info("Neighbourhood-File exists")
			loadNeighboursFromHdfs(env, NEIGHBOURS_FILE)
		} else {
			log.info("Neighbourhood-File does not exist")
			neighboursCreationMethod(NEIGHBOURS_FILE)(env)
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
			preprocessedNeighbours.map(serializeNeighboursToString _).writeAsText(fullNeighboursPath, FileSystem.WriteMode.OVERWRITE)
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

	def serializeNeighboursToString(neighbours: Neighbours): String = {
		val inString = neighbours.in.map { n => s"${n.entity}\0${n.prob}" }.mkString("\0")
		val outString = neighbours.out.map { n => s"${n.entity}\0${n.prob}" }.mkString("\0")
		s"${neighbours.entity}\t$inString\t$outString"
	}

	def buildReciprocalNeighbours(env: ExecutionEnvironment): DataSet[Neighbours] = {
		val fullNeighbours = buildFullNeighbours(env)
		fullNeighbours.map { neighbours =>
			import neighbours._
			val inSet  = in.map(_.entity).toSet
			val outSet = out.map(_.entity).toSet
			val intersection = inSet.intersect(outSet)

			val newIn = in.filter { x => intersection.contains(x.entity) }
			val inSum = newIn.map(_.prob).sum
			newIn :+ Neighbour(RandomWalkReduceGroup.NULL_NODE, 1.0 - inSum)

			val newOut = out.filter { x => intersection.contains(x.entity) }
			val outSum = newOut.map(_.prob).sum
			newOut :+ Neighbour(RandomWalkReduceGroup.NULL_NODE, 1.0 - outSum)

			Neighbours(entity, newIn, newOut)
		}
	}

	def buildFullNeighbours(env: ExecutionEnvironment): DataSet[Neighbours] = {
		val contextLinks = env.readTextFile(contextLinkProbsPath).name("ContextLinkProbs-Path").map { line =>
			val split = line.split('\t')
			ContextLink(split(0), split(1), split(2).toDouble)
		}.name("ContextLinks")
		val outgoingNeighbours = contextLinks.groupBy("from").reduceGroup { grouped =>
			val asList = grouped.toBuffer
			(asList.head.from, asList.map { contextLink => Neighbour(contextLink.to, contextLink.prob) })
		}.name("Outgoing Neighbours")
		val incomingNeighbours = contextLinks.groupBy("to").reduceGroup { grouped =>
			val asList = grouped.toBuffer
			(asList.head.to, asList.map { contextLink => Neighbour(contextLink.from, contextLink.prob) })
		}.name("Incoming Neighbours")
		val fullNeighbours = incomingNeighbours.join(outgoingNeighbours)
			.where(0)
			.equalTo(0)
			.map { joinResult => joinResult match {
					case (in, out) => Neighbours(in._1, in._2, out._2)
			}
		}.name("All-Neighbours")
		fullNeighbours
	}

	def loadNeighboursFromHdfs(env: ExecutionEnvironment, neighboursPath: String): DataSet[Neighbours] = {
		env.readTextFile(neighboursPath).map { neighboursLine =>
			val Array(entity, inString, outString) = neighboursLine.split('\t')
			val inNeighbours = inString.split("\0").grouped(2).map { case Array(ent, prob) => Neighbour(ent, prob.toDouble) }.toBuffer
			val outNeighbours = outString.split("\0").grouped(2).map { case Array(ent, prob) => Neighbour(ent, prob.toDouble) }.toBuffer
			Neighbours(entity, inNeighbours, outNeighbours)
		}.name("All-Neighbours")
	}
}

class RunTrieOverDocumentsFlatMap(params: Params) extends ReadTrieFromDiskFlatMap[InputDocument, Classifiable[ClassificationInfo]](params) {
	var tokenHitCount: Int = 1
	import CoheelLogger._

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
					out.collect(Classifiable(id, trieHit.s, context.toArray, surfaceLinkProb = trieHit.prob, info = ClassificationInfo(document.id, trieHit, POS_TAG_GROUPS.map { group => if (group.exists(tags.contains(_))) 1.0 else 0.0 })))
					tokenHitCount += 1
				}
				else {
					log.warn(s"Removing because no noun: $trieHit in >>${context.slice(20, 30).mkString(" ")}<<")
				}
			}
		}
	}
}

class ClassificationReduceGroup(params: Params) extends RichGroupReduceFunction[Classifiable[ClassificationInfo], ClassifierResult] {

	import CoheelLogger._
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
		FeatureHelper.applyCoheelFunctions(allCandidates) { featureLine =>
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
