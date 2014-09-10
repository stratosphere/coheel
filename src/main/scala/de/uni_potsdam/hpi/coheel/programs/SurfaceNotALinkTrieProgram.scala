package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.TrieBuilder
import de.uni_potsdam.hpi.coheel.datastructures.ContainsResult
import de.uni_potsdam.hpi.coheel.wiki.TextAnalyzer
import org.apache.flink.api.common.{Plan, ProgramDescription, Program}
import org.apache.flink.api.scala.ScalaPlan
import org.apache.flink.api.scala.operators.CsvOutputFormat
import OutputFiles._
import DataSetNaming._
import org.slf4s.Logging

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SurfaceNotALinkTrieProgram extends Program with ProgramDescription with Logging {
	// prepare the trie
	TrieBuilder.build()

	override def getDescription = "Counting how often a surface occurs, but not as a link. This approach uses a trie."

	override def getPlan(args: String*): Plan = {
		val wikiPages = ProgramHelper.getWikiPages()

		var c = 0
		val notALinkFreqs = wikiPages.flatMap { wikiPage =>
			log.info(f"$c%6s ${wikiPage.pageTitle}")
			c += 1
			val tokens = TextAnalyzer.tokenize(wikiPage.plainText).toArray

			val resultSurfaces = mutable.HashSet[String]()

			// each word must be checked, if it is a surface somewhere
			for (i <- 0 until tokens.size) {
				resultSurfaces ++= TrieBuilder.trie.slidingContains(tokens, i)
			}
			resultSurfaces.toIterator
		}.name("Resulting-Not-A-Link-Surfaces")

		val notALinkFreqOutput = notALinkFreqs.write(surfaceNotALinkFreqsPath,
			CsvOutputFormat[String]("\n", "\t"))
		val plan = new ScalaPlan(Seq(notALinkFreqOutput))
		plan
	}
}
