package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import de.uni_potsdam.hpi.coheel.util.Util
import de.uni_potsdam.hpi.coheel.wiki.{TokenizerHelper, FullInfoWikiPage, WikiPage}
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala.ExecutionEnvironment
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import scala.collection.mutable
import de.uni_potsdam.hpi.coheel.ml.CoheelClassifier.POS_TAG_GROUPS

class TrainingDataProgram extends CoheelProgram[String] with Serializable {

	val SAMPLE_FRACTION = if (runsOffline()) 100 else 5000

	val arguments = if (runsOffline()) List("") else List("12345", "678910")
	override def getDescription = "Wikipedia Extraction: Build training data"

	override def buildProgram(env: ExecutionEnvironment, param: String): Unit = {
		val wikiPages = readWikiPagesWithFullInfo { pageTitle =>
			pageTitle.hashCode % SAMPLE_FRACTION == 0
		}

		val currentFile = if (runsOffline()) "" else s"/$param"
		val surfaces = readSurfaces(currentFile)

		val classifiables = wikiPages
			.flatMap(new TrainingDataFlatMap)
			.withBroadcastSet(surfaces, SurfacesInTrieFlatMap.BROADCAST_SURFACES)
			.name("Links and possible links")

		val featuresPerGroup = FeatureProgramHelper.buildFeaturesPerGroup(this, classifiables)

		val trainingData = featuresPerGroup.reduceGroup(createTrainingDataGroupWise _).name("Training Data")

		// TODO: Also join surface link probs
		trainingData.writeAsText(trainingDataPath + currentFile, FileSystem.WriteMode.OVERWRITE)
	}


	/**
	 * @param candidatesIt All link candidates with scores (all LinkWithScore's have the same id).
	 */
	def createTrainingDataGroupWise(candidatesIt: Iterator[Classifiable[TrainInfo]], out: Collector[String]): Unit = {
		val allCandidates = candidatesIt.toSeq
		FeatureProgramHelper.applyCoheelFunctions(allCandidates) { featureLine =>
			import featureLine._
			def stringInfo = List(id, surfaceRepr, candidateEntity) ::: featureLine.model.modelInfo
			val output = s"${stringInfo.mkString("\t")}\t${featureLine.features.mkString("\t")}"
			out.collect(output)
		}
	}

}


class TrainingDataFlatMap extends SurfacesInTrieFlatMap[FullInfoWikiPage, Classifiable[TrainInfo]] {
	var tokenHitCount: Int = 1
	override def flatMap(wikiPage: FullInfoWikiPage, out: Collector[Classifiable[TrainInfo]]): Unit = {

		assert(wikiPage.tags.size == wikiPage.plainText.size)
		wikiPage.links.foreach { case (index, link) =>
			// In theory, the index of the link should be in the set of indices proposed by the trie:
			//    assert(hitPoints.contains(index))
			// After all, if this link was found in the first phase, its surface should be in the trie now.
			// The problem, however, is the different tokenization: When tokenizing link text, we only tokenize
			// the small text of the link, while plain text tokenization works on the entire text
			// This tokenization is sometimes different, see the following example:
			//    println(TokenizerHelper.tokenize("Robert V.").mkString(" "))            --> robert v.
			//    println(TokenizerHelper.tokenize("Robert V. The Legacy").mkString(" ")) --> robert v the legaci (dot missing)
			// TODO: This could be solved by taking the link tokenization directly from the plain text, however, this would
			//       require quite a bit of rewriting.

//			val context = for {
//				text <- Util.extractContext(wikiPage.plainText, index, CONTEXT_SPREADING)
//				pos  <- Util.extractContext(wikiPage.tags, index, CONTEXT_SPREADING)
//			} yield (text, pos)
			val contextOption = Util.extractContext(wikiPage.plainText, index)


			contextOption.foreach { context =>
				out.collect(Classifiable[TrainInfo](link.fullId, link.surfaceRepr, context.toArray, info = TrainInfo(link.source, link.destination, POS_TAG_GROUPS.map { group => if (group.exists(link.posTags.contains(_))) 1.0 else 0.0 })))
			}
		}
//		trie.findAllInWithTrieHit(wikiPage.plainText).foreach { tokenHit =>
//			val context = Util.extractContext(wikiPage.plainText, tokenHit.startIndex)
//
//			context.foreach { textContext =>
//				val tags = wikiPage.tags.slice(tokenHit.startIndex, tokenHit.startIndex + tokenHit.length).toArray
//				// TH for trie hit
//				out.collect(LinkWithContext(s"TH-${Util.id(wikiPage.pageTitle)}-$tokenHitCount", tokenHit.s, wikiPage.pageTitle, destination = "", textContext.toArray, tags))
//				tokenHitCount += 1
//			}
//		}
	}
}

