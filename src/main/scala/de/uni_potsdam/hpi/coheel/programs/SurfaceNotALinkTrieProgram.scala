package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.TrieRunner
import de.uni_potsdam.hpi.coheel.wiki.TextAnalyzer
import org.apache.flink.api.common.{Plan, ProgramDescription, Program}
import org.apache.flink.api.scala.ScalaPlan
import org.apache.flink.api.scala.operators.CsvOutputFormat
import OutputFiles._

class SurfaceNotALinkTrieProgram extends Program with ProgramDescription {

	override def getDescription = "Counting how often a surface occurs, but not as a link. This approach uses a trie."

	override def getPlan(args: String*): Plan = {

		val wikiPages = ProgramHelper.getWikiPages(10)
		val trieRunner = new TrieRunner
		val trie = trieRunner.buildTrie()

		val notALinkCounts = wikiPages.map { wikiPage =>
			val tokens = TextAnalyzer.tokenize(wikiPage.source).toArray

			for (i <- 0 until tokens.size) {
				val currentWord = tokens(i)
				trie.contains(Seq(currentWord))

			}



			(1.0, 1.0)
		}

		val notALinkCountOutput = notALinkCounts.write(surfaceNotALinkPath,
			CsvOutputFormat[(Double, Double)]("\n", "\t"))
		val plan = new ScalaPlan(Seq(notALinkCountOutput))
		plan
	}
}
