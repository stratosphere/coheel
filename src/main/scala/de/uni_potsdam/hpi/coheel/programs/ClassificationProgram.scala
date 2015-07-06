package de.uni_potsdam.hpi.coheel.programs

import java.lang.Iterable
import java.util.{Date, Collections}
import de.uni_potsdam.hpi.coheel.datastructures.TrieHit
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.ml.CoheelClassifier.POS_TAG_GROUPS

import de.uni_potsdam.hpi.coheel.io.Sample
import de.uni_potsdam.hpi.coheel.ml.CoheelClassifier
import de.uni_potsdam.hpi.coheel.util.Util
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper
import org.apache.flink.api.common.functions.{RichGroupReduceFunction, Partitioner}
import org.apache.flink.api.scala._
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem
import org.apache.log4j.Logger
import weka.classifiers.Classifier
import weka.core.SerializationHelper
import scala.collection.JavaConverters._
import org.apache.flink.util.Collector
import scala.collection.mutable

class ClassificationProgram extends NoParamCoheelProgram {

	override def getDescription: String = "CohEEL Classification"

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val documents = env.fromElements(Sample.ANGELA_MERKEL_SAMPLE_TEXT_1, Sample.ANGELA_MERKEL_SAMPLE_TEXT_2).flatMap { (text, out: Collector[InputDocument]) =>
			val tokenizer = TokenizerHelper.tokenizeWithPositionInfo(text, null)
			val document = InputDocument(Util.id(text).toString, tokenizer.getTokens, tokenizer.getTags)
			// TODO: Output several documents, to allow for partitioning on two nodes
			out.collect(document)
		}

		val currentFile = if (runsOffline()) "" else s"/678910"
		println(s"Current file is >>$currentFile<<")

		// TODO: Call this with different current files on different nodes?
		val surfaces = readSurfaces(currentFile)

		val trieHits = documents
			.flatMap(new ClassificationLinkFinderFlatMap)
			.withBroadcastSet(surfaces, SurfacesInTrieFlatMap.BROADCAST_SURFACES)
			.name("Possible links")

		val rawFeatures = FeatureProgramHelper.buildFeaturesPerGroup(this, trieHits)
		val basicClassifierResults = rawFeatures.reduceGroup(new ClassificationReduceFeatureLineGroup)

		basicClassifierResults.map { featureLine =>
			s">${featureLine.surfaceRepr}< at ${featureLine.model.trieHit} is probably ${featureLine.candidateEntity}"
		}.writeAsText(classificationPath, FileSystem.WriteMode.OVERWRITE)

		if (runsOffline()) {
			val trieHitOutput = trieHits.map { trieHit =>
				(trieHit.id, trieHit.surfaceRepr, trieHit.info.trieHit, trieHit.info.posTags.deep)
			}
			trieHitOutput.printOnTaskManager("TRIE-HITS")
		}


//		val result = documents.crossWithHuge(surfaces).flatMap { value =>
//			val (text, surfaceProb) = value
//			val surface = surfaceProb.surface
//			if (text.containsSlice(surface))
//				List((surface.mkString(" "), surfaceProb.destination, surfaceProb.prob))
//			else
//				List()
//		}
//
//		result.writeAsTsv(pageRankPath)
	}

}

class ClassificationLinkFinderFlatMap extends SurfacesInTrieFlatMap[InputDocument, Classifiable[ClassificationInfo]] {
	var tokenHitCount: Int = 1
	override def flatMap(document: InputDocument, out: Collector[Classifiable[ClassificationInfo]]): Unit = {
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
}

class ClassificationReduceFeatureLineGroup extends RichGroupReduceFunction[Classifiable[ClassificationInfo], FeatureLine[ClassificationInfo]] {

	def log = Logger.getLogger(getClass)
	var seedClassifier: CoheelClassifier = null
	var candidateClassifier: CoheelClassifier = null

	override def open(params: Configuration): Unit = {
		val seedPath = if (CoheelProgram.runsOffline()) "NaiveBayes-10FN.model" else "/home/hadoop10/data/RandomForest-10FN.model"
		val candidatePath = if (CoheelProgram.runsOffline()) "NaiveBayes-10FN.model" else "/home/hadoop10/data/RandomForest-10FP.model"

		log.info(s"Loading model with ${FreeMemory.get(true)} MB")

		val d1 = new Date
		seedClassifier      = new CoheelClassifier(SerializationHelper.read(seedPath).asInstanceOf[Classifier])
		candidateClassifier = new CoheelClassifier(SerializationHelper.read(candidatePath).asInstanceOf[Classifier])

		log.info(s"Finished model with ${FreeMemory.get(true)} MB in ${(new Date().getTime - d1.getTime) / 1000} s")
	}

	override def reduce(candidatesIt: Iterable[Classifiable[ClassificationInfo]], out: Collector[FeatureLine[ClassificationInfo]]): Unit = {
		val allCandidates = candidatesIt.asScala.toSeq
		val features = new mutable.ArrayBuffer[FeatureLine[ClassificationInfo]](allCandidates.size)
		FeatureProgramHelper.applyCoheelFunctions(allCandidates) { featureLine =>
			// TODO: Do something with this TrieInfo
			features.append(featureLine)
		}
		candidateClassifier.classifyResults(features).foreach { result =>
			out.collect(result)
		}

	}
}
