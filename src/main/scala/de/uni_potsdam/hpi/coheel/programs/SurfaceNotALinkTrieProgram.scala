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
		val notALinkCounts = wikiPages.flatMap { wikiPage =>
			log.info(f"$c%6s ${wikiPage.pageTitle}")
			c += 1
			val tokens = TextAnalyzer.tokenize(wikiPage.plainText).toArray

			val resultSurfaces = mutable.HashSet[String]()

			// each word must be checked, if it is a surface somewhere
			for (i <- 0 until tokens.size) {
				val currentCheck = ListBuffer[String](tokens(i))
				var containsResult = TrieBuilder.trie.contains(currentCheck)

				var j = 1
				// for each word, go so far until it is no intermediate node anymore
				while (containsResult.asIntermediateNode) {
					// if it is a entry, add to to result list
					if (containsResult.asEntry)
						resultSurfaces.add(currentCheck.mkString(" "))
					// expand current window, if possible
					if (i + j < tokens.size) {
						currentCheck.append(tokens(i + j))
						containsResult = TrieBuilder.trie.contains(currentCheck)
						j += 1
					} else {
						// if we reached the end of the text, we need to break
						containsResult = ContainsResult(false, false)
					}
				}
			}
			resultSurfaces.toIterator
		}.name("Resulting-Not-A-Link-Surfaces")

		val notALinkCountOutput = notALinkCounts.write(surfaceNotALinkFreqsPath,
			CsvOutputFormat[String]("\n", "\t"))
		val plan = new ScalaPlan(Seq(notALinkCountOutput))
		plan
	}
}
