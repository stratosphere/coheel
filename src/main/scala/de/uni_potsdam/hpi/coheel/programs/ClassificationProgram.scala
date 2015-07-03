package de.uni_potsdam.hpi.coheel.programs

import java.lang.Iterable
import java.util.Collections

import de.uni_potsdam.hpi.coheel.io.Sample
import de.uni_potsdam.hpi.coheel.util.Util
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper
import org.apache.flink.api.common.functions.{RichGroupReduceFunction, Partitioner}
import org.apache.flink.api.scala._
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import org.apache.flink.configuration.Configuration
import weka.classifiers.Classifier
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

		val currentFile = if (runsOffline()) "" else s"/12345"
		val surfaces = readSurfaces(currentFile)

		val potentialLinks = documents
			.flatMap(new ClassificationLinkFinderFlatMap)
			.withBroadcastSet(surfaces, SurfacesInTrieFlatMap.BROADCAST_SURFACES)
			.name("Possible links")

		val featuresPerGroup = FeatureProgramHelper.buildFeaturesPerGroup(this, potentialLinks)
		featuresPerGroup.reduceGroup { (candidatesIt, out: Collector[String]) =>
		}.printOnTaskManager("FEATURES")

		potentialLinks.map { link =>
			(link.fullId, link.surfaceRepr, link.source, link.destination, List[String](), link.posTags.deep)
		}.printOnTaskManager("LINKS")


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

class ClassificationLinkFinderFlatMap extends SurfacesInTrieFlatMap[InputDocument, LinkWithContext] {
	var tokenHitCount: Int = 1
	override def flatMap(document: InputDocument, out: Collector[LinkWithContext]): Unit = {
		trie.findAllInWithTrieHit(document.tokens).foreach { tokenHit =>
			val contextOption = Util.extractContext(document.tokens, tokenHit.startIndex)

			contextOption.foreach { case context =>
				val tags = document.tags.slice(tokenHit.startIndex, tokenHit.startIndex + tokenHit.length).toArray
				// TH for trie hit
				out.collect(LinkWithContext(s"TH-${document.id}-$tokenHitCount", tokenHit.s, "", destination = "", context.toArray, tags))
				tokenHitCount += 1
			}
		}
	}
}

class ClassificationReduceFeatureLineGroup extends RichGroupReduceFunction[LinkWithScores, String] {

	var classifier: Classifier = null

	override def open(params: Configuration): Unit = {

	}

	override def reduce(candidatesIt: Iterable[LinkWithScores], out: Collector[String]): Unit = {
		val allCandidates = candidatesIt.asScala.toSeq
		val features = new mutable.ArrayBuffer[FeatureLine](allCandidates.size)
		FeatureProgramHelper.applyCoheelFunctions(allCandidates) { featureLine =>
			val output = s"${featureLine.stringInfo.mkString("\t")}\t${featureLine.features.mkString("\t")}"
			features.append(featureLine)
			out.collect(output)
		}

	}
}
