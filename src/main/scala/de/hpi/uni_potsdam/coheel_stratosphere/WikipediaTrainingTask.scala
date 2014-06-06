package de.hpi.uni_potsdam.coheel_stratosphere

import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import eu.stratosphere.api.common.{Program, ProgramDescription, Plan}
import de.hpi.uni_potsdam.coheel_stratosphere.wiki.{TextAnalyzer, WikiPageReader, LinkExtractor}
import scala.xml.XML
import scala.io.Source


class WikipediaTrainingTask extends Program with ProgramDescription {

	override def getPlan(args: String*): Plan = {
		val currentPath = System.getProperty("user.dir")
		val input = TextFile(s"file://$currentPath/src/test/resources/wikipedia_files.txt")
		val texts = input.map { file =>
			Source.fromFile(s"src/test/resources/$file").mkString
		}

		val links = texts.flatMap { text =>
			val extractor = new LinkExtractor()
			val wikiPage = WikiPageReader.xmlToWikiPage(XML.loadString(text))
			val links = extractor.extractLinks(wikiPage)
			links
		} map {
			(_, 1)
		}

		val languageModel = texts.flatMap { text =>
			val analyzer = new TextAnalyzer
			val tokens = analyzer.analyze(text)
			tokens
		}

		val linkCounts = links.groupBy { case (link, _) => link }
					.reduce { (w1, w2) => (w1._1, w1._2 + w2._2) }
					.map { case (link, count) => (link.text, link.destination, count) }

		val countsOutput = linkCounts.write(s"file://$currentPath/testoutput/link-counts", CsvOutputFormat())
		val tokensOutput = languageModel.write(s"file://$currentPath/testoutput/language-models", CsvOutputFormat())
		val plan = new ScalaPlan(Seq(countsOutput, tokensOutput))

		plan

	}

	override def getDescription = "Training the model parameters for CohEEL."
}
