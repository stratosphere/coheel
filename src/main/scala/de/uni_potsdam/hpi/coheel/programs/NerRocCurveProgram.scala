package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.datastructures.{TrieBuilder, Trie}
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper
import org.apache.flink.api.common.{Plan, Program, ProgramDescription}
import org.slf4s.Logging

import scala.collection.immutable.HashSet
import scala.collection.mutable

class NerRocCurveProgram extends Program with ProgramDescription with Logging {

	override def getDescription = "Determining the ROC curve for the NER threshold."
	override def getPlan(args: String*): Plan = {

		val result = ProgramHelper.getWikiPages(1).flatMap { wikiPage =>
			(0.5 to 0.5 by 0.05).map { threshold => (threshold, wikiPage) }.toIterator
		}.groupBy { case (threshold, wikiPage) => threshold }
		.reduceGroup { case wikiPageIterator =>
			var trie: Trie = null

			var tp = 0
			var tn = 0
			var fp = 0
			var fn = 0
			wikiPageIterator.foreach { case (threshold, wikiPage) =>
				if (trie == null) {
					TrieBuilder.buildThresholdTrie(threshold)
					trie = TrieBuilder.thresholdTrie
				}
				val actualSurfaces = wikiPage.links.map { link => link.text }.toSet
				println(wikiPage.pageTitle)
				println(actualSurfaces)

				val tokens = TokenizerHelper.tokenizeWithPositions(wikiPage.plainText).toArray
				val resultSurfaces = mutable.HashSet[String]()
				for (i <- 0 until tokens.size) {
					val resultTokens = TrieBuilder.fullTrie.slidingContains[TokenizerHelper.Token](tokens, { token: TokenizerHelper.Token => token.word }, i)
					resultSurfaces ++= resultTokens.map { containment =>
						wikiPage.plainText.substring(containment.head.startOffset, containment.last.endOffset)
					}
				}
				resultSurfaces.map { surface => (wikiPage.pageTitle, surface) }.toIterator
			}
			1
		}
		null
	}
}
