package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.datastructures.TrieBuilder
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper
import org.apache.flink.api.common.{Plan, ProgramDescription, Program}
import org.apache.flink.api.scala.{TextFile, DataSource, ScalaPlan}
import org.apache.flink.api.scala.operators.CsvOutputFormat
import OutputFiles._
import DataSetNaming._
import org.slf4s.Logging

import scala.collection.mutable

class EntireTextSurfacesProgram extends Program with ProgramDescription with Logging {
	// prepare the trie
	TrieBuilder.buildFullTrie()

	override def getDescription = "Counting how often a surface occurs, in the entire text. This approach uses a trie."

	override def getPlan(args: String*): Plan = {
		val wikiPages = ProgramHelper.getWikiPages()

		var c = 0
		// which surfaces occur in which documents
		val entireTextSurfaces = wikiPages.flatMap { wikiPage =>
			if (c % 200000 == 0)
				log.info(f"$c%8s/11023933")
			c += 1
			val tokens = TokenizerHelper.tokenize(wikiPage.plainText).toArray

			val resultSurfaces = mutable.HashSet[String]()

			// each word and its following words must be checked, if it is a surface
			for (i <- 0 until tokens.size) {
				resultSurfaces ++= TrieBuilder.fullTrie.slidingContains(tokens, i)
			}
			resultSurfaces.map { surface => (wikiPage.pageTitle, surface) }.toIterator
		}.name("Entire-Text-Surfaces-Along-With-Document")

		val surfaceDocumentCounts = TextFile(surfaceDocumentCountsPath)

		val entireTextSurfaceCounts = entireTextSurfaces
			.groupBy { case (title, surface) => surface }
			.count()
			.map { case ((title, surface), count) => (surface, count) }
			.name("Entire-Text-Surface-Counts")

		val surfaceLinkProbs = surfaceDocumentCounts.map { line =>
			val split = line.split('\t')
			// not clear, why lines without a count occur, but they do
			try {
				if (split.size < 2)
					(split(0), 0)
				else {
					val (surface, count) = (split(0), split(1).toInt)
					(TokenizerHelper.tokenize(surface).mkString(" "), count)
				}
			} catch {
				case e: NumberFormatException =>
					(split(0), 0)
			}
		}.join(entireTextSurfaceCounts)
		.where { case (surface, _) => surface }
		.isEqualTo { case (surface, _) => surface }
		.map { case ((surface, asLinkCount), (_, entireTextCount)) =>
			(surface, entireTextCount, asLinkCount.toDouble / entireTextCount.toDouble)
		}

		val entireTextSurfacesOutput = entireTextSurfaces.write(entireTextSurfacesPath,
			CsvOutputFormat[(String, String)]("\n", "\t"))
		val entireTextSurfacesCountsOutput = entireTextSurfaceCounts.write(entireTextSurfaceCountsPath,
			CsvOutputFormat[(String, Int)]("\n", "\t"))
		val surfaceLinkProbsOutput = surfaceLinkProbs.write(surfaceLinkProbsPath,
			CsvOutputFormat[(String, Int, Double)]("\n", "\t"))
		val plan = new ScalaPlan(Seq(entireTextSurfacesOutput, entireTextSurfacesCountsOutput, surfaceLinkProbsOutput))
		plan
	}
}
