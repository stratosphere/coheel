package de.uni_potsdam.hpi.coheel.programs

import java.lang

import de.uni_potsdam.hpi.coheel.datastructures.{TrieBuilder, Trie}
import de.uni_potsdam.hpi.coheel.wiki.{WikiPage, TokenizerHelper}
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper.Token
import org.apache.flink.api.common.functions.{RichGroupReduceFunction}
import org.apache.flink.api.common.{Plan, ProgramDescription}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._
import org.slf4s.Logging
import OutputFiles._

import scala.collection.mutable

class NerRocCurveProgram extends CoheelProgram with ProgramDescription with Logging {

	TrieBuilder.buildFullTrie()

	override def getDescription = "Determining the ROC curve for the NER threshold."

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val wikiPages = ProgramHelper.filterNormalPages(ProgramHelper.getWikiPages(env))
		val rocValues = env.fromElements[(Int, Int)]((2, 2))

		val foo = wikiPages.iterate(100) { iterationInput =>
			iterationInput.reduceGroup(new RichGroupReduceFunction[WikiPage, Int] {
				var results: mutable.Buffer[(Int, Int)] = _
				override def open(config: Configuration): Unit = {
					results = getRuntimeContext.getBroadcastVariable[(Int, Int)]("ROC-VALUES").asScala
				}

				override def reduce(p1: lang.Iterable[WikiPage], p2: Collector[Int]): Unit = {
					this.results.+=((1, 1))
//					p2.collect(1)
//					results.add((1, 1))
				}
			}).withBroadcastSet(rocValues, "ROC-VALUES")
			iterationInput
		}.reduceGroup { _ =>
			(3, 3)
		}

		foo.print()
		rocValues.print()
//		useless.writeAsTsv(nerRocCurvePath)



//		val rocValues = thresholds.reduceGroup { wikiPagesIt =>
//			val wikiPages = wikiPagesIt.toList
//			val threshold = wikiPages.head.threshold
//			println(f"Working on threshold $threshold%.2f.")
//			TrieBuilder.buildThresholdTrie(threshold)
//			val thresholdTrie: Trie = TrieBuilder.thresholdTrie
//
//			var tp = 0
//			var tn = 0
//			var fp = 0
//			var fn = 0
//			wikiPages.foreach { pageThreshold =>
//				val wikiPage = pageThreshold.wikiPage
//				// determine the actual surfaces, from the real wikipedia article
//				val actualSurfaces = wikiPage.links.map { link => link.surface }.toSet
////				log.warn(wikiPage.pageTitle)
//
//				// determine potential surfaces, i.e. the surfaces that the NER would return for the current
//				// threshold
//				val potentialSurfaces = determinePotentialSurfaces(wikiPage, thresholdTrie)
//				// determine the potential surfaces for a trie with all surfaces, i.e. the threshold is
//				// zero
//				val fullPotentialSurfaces = determinePotentialSurfaces(wikiPage, TrieBuilder.fullTrie)
//				// TPs are those surfaces, which are actually in the text and our system would return it (for the
//				// current trie/threshold)
//				tp += actualSurfaces.intersect(potentialSurfaces).size
//				// TNs are those surfaces, which would be returned for threshold zero, but aren't because of the
//				// current threshold, and are no actual surfaces.
//				tn += fullPotentialSurfaces.diff(potentialSurfaces).diff(actualSurfaces).size
//				// FPs are those surfaces, which are returned but are not actually surfaces
//				fp += potentialSurfaces.diff(actualSurfaces).size
//				// FN are those surfaces, which are actual surfaces, but are not returned
//				fn += actualSurfaces.diff(potentialSurfaces).size
//			}
//			// output threshold as a string, because otherwise rounding errors occur
//			(f"$threshold%.2f", tp, tn, fp, fn, tp.toDouble / (tp + fn), fp.toDouble / (fp + tn))
//		}.name("ROC-Curve-Values")

	}

	def determinePotentialSurfaces(wikiPage: WikiPage, trie: Trie): Set[String] =  {
		var potentialSurfaces = Set[String]()

		val tokens = TokenizerHelper.tokenizeWithPositions(wikiPage.plainText).toArray

		for (i <- 0 until tokens.size) {
			val resultTokens = trie.slidingContains[Token](tokens, { token: Token => token.word }, i)
			potentialSurfaces ++= resultTokens.map { containment =>
				// extract the real world surfaces, by taking the whole string from the start of the first
				// part to the end of the last part
				wikiPage.plainText.substring(containment.head.startOffset, containment.last.endOffset)
			}
		}
		potentialSurfaces
	}
}
