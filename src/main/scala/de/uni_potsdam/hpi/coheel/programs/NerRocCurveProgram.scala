package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.datastructures.{TrieBuilder, Trie}
import de.uni_potsdam.hpi.coheel.wiki.{WikiPage, TokenizerHelper}
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper.Token
import org.apache.flink.api.common.{Plan, Program, ProgramDescription}
import org.apache.flink.api.scala.ScalaPlan
import org.apache.flink.api.scala.operators.CsvOutputFormat
import org.slf4s.Logging
import OutputFiles._
import DataSetNaming._

class NerRocCurveProgram extends Program with ProgramDescription with Logging {

	TrieBuilder.buildFullTrie()

	override def getDescription = "Determining the ROC curve for the NER threshold."
	override def getPlan(args: String*): Plan = {

		val thresholds = ProgramHelper.getWikiPages(1).flatMap { wikiPage =>
			println(s"Reading ${wikiPage.pageTitle}.")
			// introduce all thresholds for each wikipage
			(0.5 to 0.5 by 0.05).map { threshold =>
				(threshold, wikiPage)
			}.toIterator
		// group all computations for one threshold together, so that the trie needs to be built only once
		}.groupBy { case (threshold, wikiPage) => threshold }


		val rocValues = thresholds.reduceGroup { case wikiPages =>
			println("Reducing a group.")
			var trie: Trie = null

			var tp = 0
			var tn = 0
			var fp = 0
			var fn = 0
			wikiPages.map { case (threshold, wikiPage) =>
				if (trie == null) {
					// build threshold trie on first entry
					TrieBuilder.buildThresholdTrie(threshold)
					trie = TrieBuilder.thresholdTrie
				}
				// determine the actual surfaces, from the real wikipedia article
				val actualSurfaces = wikiPage.links.map { link => link.text }.toSet
				println(wikiPage.pageTitle)
				println(actualSurfaces)

				// determine potential surfaces, i.e. the surfaces that the NER would return for the current
				// threshold
				val potentialSurfaces = determinePotentialSurfaces(wikiPage)

				(threshold, actualSurfaces.size, potentialSurfaces.size)
			}
		}.name("ROC-Curve-Values")

		val rocValuesOutput = rocValues.write(nerRocCurvePath, CsvOutputFormat("\n", "\t"))
		new ScalaPlan(Seq(rocValuesOutput))
	}

	def determinePotentialSurfaces(wikiPage: WikiPage): Set[String] =  {
		var potentialSurfaces = Set[String]()

		val tokens = TokenizerHelper.tokenizeWithPositions(wikiPage.plainText).toArray

		for (i <- 0 until tokens.size) {
			val resultTokens = TrieBuilder.fullTrie.slidingContains[Token](tokens, { token: Token => token.word }, i)
			potentialSurfaces ++= resultTokens.map { containment =>
				// extract the real world surfaces, by taking the whole string from the start of the first
				// part to the end of the last part
				wikiPage.plainText.substring(containment.head.startOffset, containment.last.endOffset)
			}
		}
		potentialSurfaces
	}
}
