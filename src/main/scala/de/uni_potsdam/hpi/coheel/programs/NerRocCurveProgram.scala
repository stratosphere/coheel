package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.datastructures.{TrieBuilder, Trie}
import org.apache.flink.api.common.{Plan, Program, ProgramDescription}
import org.slf4s.Logging

class NerRocCurveProgram extends Program with ProgramDescription with Logging {

	override def getDescription = "Determining the ROC curve for the NER threshold."
	override def getPlan(args: String*): Plan = {

		ProgramHelper.getWikiPages(10000).flatMap { wikiPage =>
			(0.0 to 1.0 by 0.05).map { threshold => (threshold, wikiPage) }.toIterator
		}.groupBy { case (threshold, wikiPage) => threshold }
		.reduceGroup { case wikiPageIterator =>
			var trie: Trie = _

			var tp = 0
			var tn = 0
			var fp = 0
			var fn = 0
			wikiPageIterator.foreach { case (threshold, wikiPage) =>
				if (trie == null) {
					TrieBuilder.buildThresholdTrie(threshold)
					trie = TrieBuilder.thresholdTrie
				}
				wikiPage.
			}
		}
		null
	}
}
