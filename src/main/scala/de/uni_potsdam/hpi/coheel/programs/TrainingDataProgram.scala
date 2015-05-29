package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.programs.DataClasses.{EntireTextSurfaceCounts, TrainingData}
import de.uni_potsdam.hpi.coheel.wiki.WikiPage
import org.apache.flink.api.scala.ExecutionEnvironment
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._


class TrainingDataProgram extends CoheelProgram[Int] {
	val params = if (runsOffline()) List(-1) else 1 to 10
	override def getDescription = "Wikipedia Extraction: Build training data"

	override def buildProgram(env: ExecutionEnvironment, param: Int): Unit = {
		val wikiPages = getWikiPages(useContext = true, usePos = true)

		val currentFile = if (runsOffline()) "" else s"/$param"
		val surfaces    = getSurfaces(currentFile)

//		val newScores = surfaceLinkProbs.join(scores)
//			.where(0)
//			.equalTo(0)
//			.map { joinResult =>
//			joinResult match {
//				case ((_, _, _, surfaceLinkProb), (_, values)) =>
//					values.toList.take(4).mkString("\t") + s"\t$surfaceLinkProb\t" + values.drop(4).mkString("\t")
//			}
//		}

//		val trainingData = plainTexts
//			.flatMap(new FindEntireTextSurfacesFlatMap)
//			.withBroadcastSet(surfaces, SurfacesInTrieFlatMap.BROADCAST_SURFACES)
//			.name("Entire-Text-Surfaces-Along-With-Document")
//
//		trainingData.writeAsText(trainingDataPath)
	}


	def foo(): Unit = {
//		val linksWithContext = wikiPages.flatMap { wikiPage =>
//			if (wikiPage.pageTitle.hashCode % SAMPLE_FRACTION == 0)
//				wikiPage.links
//			else
//				Nil
//		}
//
//		val linkCandidates = linksWithContext.join(surfaceProbs)
//			.where { linkWithContext => linkWithContext.surfaceRepr }
//			.equalTo { surfaceProb => surfaceProb._1 }
//			.map { joinResult => joinResult match {
//			case (linkWithContext, (_, candidateEntity, prob)) =>
//				import linkWithContext._
//				LinkCandidate(id, surfaceRepr, posTags.exists(_.startsWith("N")), posTags.exists(_.startsWith("V")),
//					source, destination, candidateEntity, prob, context)
//		}
//		}
//
//		val baseScores = linkCandidates.join(languageModels)
//			.where("candidateEntity")
//			.equalTo("pageTitle")
//			.map { joinResult => joinResult match {
//			case (linkCandidate, languageModel) =>
//				val modelSize = languageModel.model.size
//				val contextProb = linkCandidate.context.map { word =>
//					Math.log(languageModel.model.get(word) match {
//						case Some(prob) => prob
//						case None => 1.0 / modelSize
//					})
//				}.sum
//
//				import linkCandidate._
//
//				val np = if (nounPhrase) 1.0 else 0.0
//				val vp = if (verbPhrase) 1.0 else 0.0
//				LinkWithScores(fullId, surfaceRepr, source, destination, candidateEntity, np, vp, prob, contextProb)
//		}
//		}
//		val trainingData = baseScores.groupBy("fullId")
//			.reduceGroup(applySecondOrderCoheelFunctions _)
//
//		trainingData.writeAsTsv(trainingDataPath)
	}
}


class TrainingDataFlatMap extends SurfacesInTrieFlatMap[WikiPage, TrainingData] {
	override def flatMap(wikiPage: WikiPage, out: Collector[TrainingData]): Unit = {

	}
}

