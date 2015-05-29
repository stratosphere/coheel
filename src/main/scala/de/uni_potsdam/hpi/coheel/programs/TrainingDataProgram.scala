package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.programs.DataClasses.{EntireTextSurfaceCounts, TrainingData}
import de.uni_potsdam.hpi.coheel.wiki.{FullInfoWikiPage, WikiPage}
import org.apache.flink.api.scala.ExecutionEnvironment
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

class TrainingDataProgram extends CoheelProgram[Int] with Serializable {

	val SAMPLE_FRACTION = if (runsOffline()) 100 else 10000

	val params = if (runsOffline()) List(-1) else 1 to 10
	override def getDescription = "Wikipedia Extraction: Build training data"

	override def buildProgram(env: ExecutionEnvironment, param: Int): Unit = {
		val wikiPages = getWikiPagesWithFullInfo { wikiPage =>
			wikiPage.pageTitle.hashCode % SAMPLE_FRACTION == 0
		}

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

		val trainingData = wikiPages
			.flatMap(new TrainingDataFlatMap)
			.withBroadcastSet(surfaces, SurfacesInTrieFlatMap.BROADCAST_SURFACES)
			.name("Training-Data")

		trainingData.writeAsText(trainingDataPath)
	}


	def foo(): Unit = {
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


//			val CONTEXT_SPREADING = 25
//			val contextOption =  Util.extractContext(tokenizerResult.getTokens, position, CONTEXT_SPREADING)

	}
}


class TrainingDataFlatMap extends SurfacesInTrieFlatMap[FullInfoWikiPage, TrainingData] {
	override def flatMap(wikiPage: FullInfoWikiPage, out: Collector[TrainingData]): Unit = {
		val hits = trie.findAllInWithTrieHit(wikiPage.plainText)
		hits.foreach { hit =>
			println(hit)
		}

	}
}

