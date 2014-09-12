package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.TrieBuilder
import de.uni_potsdam.hpi.coheel.wiki.TextAnalyzer
import org.apache.flink.api.common.{Plan, ProgramDescription, Program}
import org.apache.flink.api.scala.ScalaPlan
import org.apache.flink.api.scala.operators.CsvOutputFormat
import OutputFiles._
import DataSetNaming._
import org.slf4s.Logging

import scala.collection.mutable

class EntireTextSurfacesProgram extends Program with ProgramDescription with Logging {
	// prepare the trie
	TrieBuilder.build()

	override def getDescription = "Counting how often a surface occurs, in the entire text. This approach uses a trie."

	override def getPlan(args: String*): Plan = {
		val wikiPages = ProgramHelper.getWikiPages()

		var c = 0
		val entireTextSurfaces = wikiPages.flatMap { wikiPage =>
			log.info(f"$c%6s ${wikiPage.pageTitle}")
			c += 1
			val tokens = TextAnalyzer.tokenize(wikiPage.plainText).toArray

			val resultSurfaces = mutable.HashSet[String]()

			// each word and its following words must be checked, if it is a surface
			for (i <- 0 until tokens.size) {
				resultSurfaces ++= TrieBuilder.trie.slidingContains(tokens, i)
			}
			resultSurfaces.map { surface => (wikiPage.pageTitle, surface) }.toIterator
		}.name("Entire-Text-Surfaces-Along-With-Document")

		val entireTextFreqs = entireTextSurfaces
			.groupBy { case (title, surface) => surface }
			.count()
			.map { case ((title, surface), count) => (surface, count) }
			.name("Entire-Text-Surface-Frequencies")

		val entireTextSurfacesOutput = entireTextSurfaces.write(entireTextSurfacesPath,
			CsvOutputFormat[(String, String)]("\n", "\t"))
		val entireTextSurfacesFreqsOutput = entireTextFreqs.write(entireTextSurfaceFreqsPath,
			CsvOutputFormat[(String, Int)]("\n", "\t"))
		val plan = new ScalaPlan(Seq(entireTextSurfacesOutput, entireTextSurfacesFreqsOutput))
		plan
	}
}
