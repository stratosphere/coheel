package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.ml.SecondOrderFeatures
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import de.uni_potsdam.hpi.coheel.util.Util
import de.uni_potsdam.hpi.coheel.wiki.{TokenizerHelper, FullInfoWikiPage, WikiPage}
import org.apache.flink.api.scala.ExecutionEnvironment
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import scala.collection.mutable

import scala.util.hashing.MurmurHash3

class TrainingDataProgram extends CoheelProgram[Int] with Serializable {

	val SAMPLE_FRACTION = if (runsOffline()) 100 else 10000

	val params = if (runsOffline()) List(-1) else 1 to 10
	override def getDescription = "Wikipedia Extraction: Build training data"

	override def buildProgram(env: ExecutionEnvironment, param: Int): Unit = {
		val wikiPages = readWikiPagesWithFullInfo { pageTitle =>
			pageTitle.hashCode % SAMPLE_FRACTION == 0
		}

		val currentFile = if (runsOffline()) "" else s"/$param"
		val surfaces = readSurfaces(currentFile)
		val surfaceProbs = readSurfaceProbs()
		val languageModels = readLanguageModels()

		val linksWithContext = wikiPages
			.flatMap(new TrainingDataFlatMap)
			.withBroadcastSet(surfaces, SurfacesInTrieFlatMap.BROADCAST_SURFACES)
			.name("Training-Data")

		val posTagGroups = Array(
			List("NN", "NNS"),
			List("NNP", "NNPS"),
			List("JJ", "JJR", "JJS"),
			List("VB", "VBD", "VBG", "VBN", "VBP", "VBZ"),
			List("CD"),
			List("SYM"),
			List("WDT", "WP", "WP$", "WRB")
		)


		val linkCandidates = linksWithContext.join(surfaceProbs)
			.where { linkWithContext => linkWithContext.surfaceRepr }
			.equalTo { surfaceProb => surfaceProb.surface }
			.map { joinResult => joinResult match {
			case (linkWithContext, SurfaceProb(_, candidateEntity, prob)) =>
				import linkWithContext._
				LinkCandidate(fullId, surfaceRepr, source, destination, candidateEntity, prob, context,
					posTagGroups.map { group => if (group.exists(posTags.contains(_))) 1 else 0 })
		}
		}
		val baseScores = linkCandidates.join(languageModels)
			.where("candidateEntity")
			.equalTo("pageTitle")
			.map { joinResult => joinResult match {
				case (linkCandidate, languageModel) =>
					val modelSize = languageModel.model.size
					val contextProb = linkCandidate.context.map { word =>
						Math.log(languageModel.model.get(word) match {
							case Some(prob) => prob
							case None => 1.0 / modelSize
						})
					}.sum

					import linkCandidate._

					LinkWithScores(fullId, surfaceRepr, source, destination, candidateEntity, posTagsScores.map(_.toDouble), prob, contextProb)
			}
		}
		val trainingData = baseScores.groupBy("fullId")
			.reduceGroup(applySecondOrderCoheelFunctions _)

		trainingData.writeAsText(trainingDataPath, FileSystem.WriteMode.OVERWRITE)
	}


	/**
	 * @param candidatesIt All link candidates with scores (all LinkWithScore's have the same id).
	 */
	def applySecondOrderCoheelFunctions(candidatesIt: Iterator[LinkWithScores],
	                                    out: Collector[String]): Unit = {
		val allCandidates = candidatesIt.toSeq
		val promOrder = allCandidates.sortBy(-_.promScore)
		val contextOrder = allCandidates.sortBy(-_.contextScore)
		if (allCandidates.size > 1) {
			val promRank       = SecondOrderFeatures.rank.apply(promOrder)(_.promScore)
			val promDeltaTops  = SecondOrderFeatures.deltaTop.apply(promOrder)(_.promScore)
			val promDeltaSuccs = SecondOrderFeatures.deltaSucc.apply(promOrder)(_.promScore)
			val contextRank       = SecondOrderFeatures.rank.apply(contextOrder)(_.contextScore)
			val contextDeltaTops  = SecondOrderFeatures.deltaTop.apply(contextOrder)(_.contextScore)
			val contextDeltaSuccs = SecondOrderFeatures.deltaSucc.apply(contextOrder)(_.contextScore)

			promOrder.zipWithIndex.foreach { case (candidate, i) =>
				val positiveInstance = candidate.destination == candidate.candidateEntity
				import candidate._
				val posStrings = posTagScores.mkString("\t")
				val output = s"$fullId\t$surfaceRepr\t$source\t$candidateEntity\t$posStrings\t" +
						s"$promScore\t${promRank(i)}\t${promDeltaTops(i)}\t${promDeltaSuccs(i)}\t" +
						s"$contextScore\t${contextRank(i)}\t${contextDeltaTops(i)}\t${contextDeltaSuccs(i)}\t" +
						s"$positiveInstance"
				out.collect(output)
			}
		}
	}
}


class TrainingDataFlatMap extends SurfacesInTrieFlatMap[FullInfoWikiPage, LinkWithContext] {
	var tokenHitCount: Int = 1
	override def flatMap(wikiPage: FullInfoWikiPage, out: Collector[LinkWithContext]): Unit = {
		val CONTEXT_SPREADING = 25

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

			val context = for {
				text <- Util.extractContext(wikiPage.plainText, index, CONTEXT_SPREADING)
				pos  <- Util.extractContext(wikiPage.tags, index, CONTEXT_SPREADING)
			} yield (text, pos)

			context.foreach { case (textContext, posContext) =>
				out.collect(LinkWithContext(link.fullId, link.surfaceRepr, link.source, link.destination, textContext.toArray, posContext.toArray))
			}
		}
		trie.findAllInWithTrieHit(wikiPage.plainText).foreach { tokenHit =>
			val context = for {
				text <- Util.extractContext(wikiPage.plainText, tokenHit.offset, CONTEXT_SPREADING)
				pos  <- Util.extractContext(wikiPage.tags, tokenHit.offset, CONTEXT_SPREADING)
			} yield (text, pos)

			context.foreach { case (textContext, posContext) =>
				out.collect(LinkWithContext(s"TH-${MurmurHash3.stringHash(wikiPage.pageTitle).toLong - Int.MinValue}-$tokenHitCount", tokenHit.s, wikiPage.pageTitle, destination = "", textContext.toArray, posContext.toArray))
				tokenHitCount += 1
			}
		}
	}
}

