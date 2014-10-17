package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.datastructures.{TrieBuilder, Trie}
import de.uni_potsdam.hpi.coheel.wiki.{WikiPage, TokenizerHelper}
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper.Token
import org.apache.flink.api.common.{Plan, ProgramDescription}
import org.apache.flink.api.scala._
import org.slf4s.Logging
import OutputFiles._

class NerRocCurveProgram extends CoheelProgram with ProgramDescription with Logging {

	TrieBuilder.buildFullTrie()

	override def getDescription = "Determining the ROC curve for the NER threshold."

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		case class WikiPageThreshold(threshold: Double, wikiPage: WikiPage)
		val thresholds = ProgramHelper.filterNormalPages(ProgramHelper.getWikiPages(env)).flatMap { wikiPage =>
			// introduce all thresholds for each wikipage
			(0.00 to 1.00 by 0.01).map { threshold =>
				WikiPageThreshold(threshold, wikiPage)
			}.toIterator
		// group all computations for one threshold together, so that the trie needs to be built only once
		}.groupBy { _.threshold }


		val rocValues = thresholds.reduceGroup { wikiPagesIt =>
			val wikiPages = wikiPagesIt.toList
			val threshold = wikiPages.head.threshold
			println(f"Working on threshold $threshold%.2f.")
			TrieBuilder.buildThresholdTrie(threshold)
			val thresholdTrie: Trie = TrieBuilder.thresholdTrie

			var tp = 0
			var tn = 0
			var fp = 0
			var fn = 0
			wikiPages.foreach { pageThreshold =>
				val wikiPage = pageThreshold.wikiPage
				// determine the actual surfaces, from the real wikipedia article
				val actualSurfaces = wikiPage.links.map { link => link.surface }.toSet
//				log.warn(wikiPage.pageTitle)

				// determine potential surfaces, i.e. the surfaces that the NER would return for the current
				// threshold
				val potentialSurfaces = determinePotentialSurfaces(wikiPage, thresholdTrie)
				// determine the potential surfaces for a trie with all surfaces, i.e. the threshold is
				// zero
				val fullPotentialSurfaces = determinePotentialSurfaces(wikiPage, TrieBuilder.fullTrie)
				// TPs are those surfaces, which are actually in the text and our system would return it (for the
				// current trie/threshold)
				tp += actualSurfaces.intersect(potentialSurfaces).size
				// TNs are those surfaces, which would be returned for threshold zero, but aren't because of the
				// current threshold, and are no actual surfaces.
				tn += fullPotentialSurfaces.diff(potentialSurfaces).diff(actualSurfaces).size
				// FPs are those surfaces, which are returned but are not actually surfaces
				fp += potentialSurfaces.diff(actualSurfaces).size
				// FN are those surfaces, which are actual surfaces, but are not returned
				fn += actualSurfaces.diff(potentialSurfaces).size
			}
			// output threshold as a string, because otherwise rounding errors occur
			(f"$threshold%.2f", tp, tn, fp, fn, tp.toDouble / (tp + fn), fp.toDouble / (fp + tn))
		}.name("ROC-Curve-Values")

		rocValues.writeAsTsv(nerRocCurvePath)
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
