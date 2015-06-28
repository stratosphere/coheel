package de.uni_potsdam.hpi.coheel.programs

import java.util.Collections

import de.uni_potsdam.hpi.coheel.io.Sample
import de.uni_potsdam.hpi.coheel.util.Util
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper
import org.apache.flink.api.scala._
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import org.apache.flink.util.Collector
import scala.collection.mutable

class ClassificationProgram extends NoParamCoheelProgram {

	override def getDescription: String = "CohEEL Classification"

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val documents = env.fromElements(Sample.ANGELA_MERKEL_SAMPLE_TEXT_1, Sample.ANGELA_MERKEL_SAMPLE_TEXT_2).map { text =>
			// TODO
			TokenizerHelper.tokenizeWithPositionInfo(text, null).getTokens
		}

		val currentFile = if (runsOffline()) "" else s"/12345"
		val surfaces = readSurfaces(currentFile)

		val potentialLinks = documents
			.flatMap(new ClassificationLinkFinderFlatMap)
			.withBroadcastSet(surfaces, SurfacesInTrieFlatMap.BROADCAST_SURFACES)
			.name("Possible links")

		potentialLinks.printOnTaskManager("FOO")

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

class ClassificationLinkFinderFlatMap extends SurfacesInTrieFlatMap[mutable.ArrayBuffer[String], LinkWithContext] {
	var tokenHitCount: Int = 1
	override def flatMap(document: mutable.ArrayBuffer[String], out: Collector[LinkWithContext]): Unit = {
		trie.findAllInWithTrieHit(document).foreach { tokenHit =>
			val contextOption = Util.extractContext(document, tokenHit.offset)

			contextOption.foreach { case context =>
				// TH for trie hit
				// TODO: Fix tags!
				out.collect(LinkWithContext(s"TH-$tokenHitCount", tokenHit.s, "TODO", destination = "", context.toArray, List("").toArray))
				tokenHitCount += 1
			}
		}
	}
}
