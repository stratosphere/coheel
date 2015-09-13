package de.uni_potsdam.hpi.coheel.programs

import java.io.File
import java.lang.Iterable
import java.util.Date

import de.uni_potsdam.hpi.coheel.datastructures.NewTrie
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
import org.apache.flink.util.Collector
import org.apache.log4j.Logger
import weka.classifiers.Classifier
import weka.core.SerializationHelper

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source

class DocumentPartitioner extends Partitioner[String] {
	override def partition(key: String, numPartitions: Int): Int = {
		if (key == "12345")
			0
		else if (key == "678910") {
			if (CoheelProgram.runsOffline()) 0 else 8
		} else
			throw new RuntimeException("Unknown surface file")
	}
}
class ClassificationProgram extends NoParamCoheelProgram {

	override def getDescription: String = "CohEEL Classification"

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val documents = env.fromElements(Sample.ANGELA_MERKEL_SAMPLE_TEXT_1, Sample.ANGELA_MERKEL_SAMPLE_TEXT_2).name("Documents")
		val tokenizedDocuments = documents.flatMap { (text, out: Collector[InputDocument]) =>
			val tokenizer = TokenizerHelper.tokenizeWithPositionInfo(text, null)
			val id = Util.id(text).toString
			val tokens = tokenizer.getTokens
			val tags = tokenizer.getTags
			out.collect(InputDocument(id, tokens, tags, "12345"))
			out.collect(InputDocument(id, tokens, tags, "678910"))
		}

		val partitioned = tokenizedDocuments.partitionCustom(new DocumentPartitioner, "surfaceFile")

		val trieHits = partitioned
			.flatMap(new ClassificationLinkFinderFlatMap)
			.name("Possible links")

		val rawFeatures = FeatureProgramHelper.buildFeaturesPerGroup(this, trieHits)
		val basicClassifierResults = rawFeatures.reduceGroup(new ClassificationFeatureLineReduceGroup)


		rawFeatures.reduceGroup { (classifiablesIt, out: Collector[(String, String, Double, Double, Int, Int)]) =>
			classifiablesIt.foreach { classifiable =>
				import classifiable._
				out.collect((surfaceRepr, candidateEntity, surfaceProb, contextProb, info.trieHit.startIndex, info.trieHit.length))
			}
		}.writeAsTsv(rawFeaturesPath)

		basicClassifierResults.map { featureLine =>
			(featureLine.surfaceRepr, featureLine.candidateEntity, featureLine.model.trieHit)
		}.writeAsTsv(classificationPath)

		val trieHitOutput = trieHits.map { trieHit =>
			(trieHit.id, trieHit.surfaceRepr, trieHit.info.trieHit, trieHit.info.posTags.deep)
		}
		trieHitOutput.writeAsTsv(trieHitPath)

	}

}

class ClassificationLinkFinderFlatMap extends RichFlatMapFunction[InputDocument, Classifiable[ClassificationInfo]] {
	var tokenHitCount: Int = 1

	def log = Logger.getLogger(getClass)
	var trie: NewTrie = _
	var fileName: String = _

	override def open(params: Configuration): Unit = {
		val surfacesFile = if (CoheelProgram.runsOffline()) {
			new File("output/surface-probs.wiki")
		} else {
			val file = new File("/home/hadoop10/data/coheel/12345")
			if (file.exists())
				file
			else
				new File("/home/hadoop10/data/coheel/678910")
		}
		assert(surfacesFile.exists())
		val surfaces = Source.fromFile(surfacesFile).getLines().flatMap { line =>
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
		if (!CoheelProgram.runsOffline() && fileName == document.surfaceFile)
			log.error(s"Filename '$fileName' is not equal to surface file '${document.surfaceFile}'")
		trie.findAllInWithTrieHit(document.tokens).foreach { trieHit =>
			val contextOption = Util.extractContext(document.tokens, trieHit.startIndex)

			contextOption.foreach { case context =>
				val tags = document.tags.slice(trieHit.startIndex, trieHit.startIndex + trieHit.length).toArray
				// TH for trie hit
				val id = s"TH-${document.id}-$tokenHitCount"
				out.collect(Classifiable(id, trieHit.s, context.toArray, info = ClassificationInfo(trieHit, POS_TAG_GROUPS.map { group => if (group.exists(tags.contains(_))) 1.0 else 0.0 })))
				tokenHitCount += 1
			}
		}
	}

	override def close(): Unit = {
		trie = null
	}
}

class ClassificationFeatureLineReduceGroup extends RichGroupReduceFunction[Classifiable[ClassificationInfo], FeatureLine[ClassificationInfo]] {

	def log = Logger.getLogger(getClass)
	var seedClassifier: CoheelClassifier = null
	var candidateClassifier: CoheelClassifier = null

	override def open(params: Configuration): Unit = {
		val seedPath = if (CoheelProgram.runsOffline()) "NaiveBayes-10FN.model" else "/home/hadoop10/data/coheel/RandomForest-10FN.model"
		val candidatePath = if (CoheelProgram.runsOffline()) "NaiveBayes-10FN.model" else "/home/hadoop10/data/coheel/RandomForest-10FP.model"

		log.info(s"Loading models with ${FreeMemory.get(true)} MB")

		val d1 = new Date
		seedClassifier      = new CoheelClassifier(SerializationHelper.read(seedPath).asInstanceOf[Classifier])
		candidateClassifier = new CoheelClassifier(SerializationHelper.read(candidatePath).asInstanceOf[Classifier])

		log.info(s"Finished model with ${FreeMemory.get(true)} MB in ${(new Date().getTime - d1.getTime) / 1000} s")
	}

	override def reduce(candidatesIt: Iterable[Classifiable[ClassificationInfo]], out: Collector[FeatureLine[ClassificationInfo]]): Unit = {
		val allCandidates = candidatesIt.asScala.toSeq
		val features = new mutable.ArrayBuffer[FeatureLine[ClassificationInfo]](allCandidates.size)
		FeatureProgramHelper.applyCoheelFunctions(allCandidates) { featureLine =>
			features.append(featureLine)
		}
		candidateClassifier.classifyResults(features).foreach { result =>
			out.collect(result)
		}
	}

	override def close(): Unit = {
		seedClassifier = null
		candidateClassifier = null
	}
}
