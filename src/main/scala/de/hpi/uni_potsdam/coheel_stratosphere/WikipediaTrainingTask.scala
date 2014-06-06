package de.hpi.uni_potsdam.coheel_stratosphere

import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import eu.stratosphere.api.common.{Program, ProgramDescription, Plan}
import de.hpi.uni_potsdam.coheel_stratosphere.wiki.{Link, TextAnalyzer, WikiPageReader, LinkExtractor}
import scala.xml.XML
import scala.io.Source


class WikipediaTrainingTask extends Program with ProgramDescription {

	override def getDescription = "Training the model parameters for CohEEL."

	lazy val currentPath = System.getProperty("user.dir")
	// input files, file with the names of the test wikipedia articles
	lazy val wikipediaFilesPath = s"file://$currentPath/src/test/resources/wikipedia_files.txt"
	// outputs files
	lazy val surfaceCountsPath     = s"file://$currentPath/testoutput/surface-counts"
	lazy val contextLinkCountsPath = s"file://$currentPath/testoutput/context-link-counts"
	lazy val languageModelsPath    = s"file://$currentPath/testoutput/language-models"

	/**
	 * Builds a plan to create the three main data structures CohEEL needs.
	 * <ul>
	 *   <li> How often is each entity mention under a certain surface.
	 *   <li> How often does entity A link to entity B?
	 *   <li> How often does each word occur in an entity's text.
	 * @param args Not used.
	 */
	override def getPlan(args: String*): Plan = {
		val input = TextFile(wikipediaFilesPath)
		val pageSource = input.map { file =>
			val pageSource = Source.fromFile(s"src/test/resources/$file").mkString
			pageSource
		}

		val (linkCountPlan, linkContextCountPlan)   = buildLinkCountPlan(pageSource)
		val wordCountPlan   = buildWordCountPlan(pageSource)
		val plan = new ScalaPlan(Seq(linkCountPlan, wordCountPlan, linkContextCountPlan))

		plan
	}

	/**
	 * Builds two plans:
	 * <ul>
	 *   <li> the plan who counts how often one document links to another
	 *   <li> the plan who counts how often a link occurs under a certain surface
	 */
	def buildLinkCountPlan(pageSource: DataSet[String]):
		(ScalaSink[(String, String, Int)], ScalaSink[(String, String, Int)]) = {
		val links = pageSource.flatMap { pageSource =>
			// extract all links
			val extractor = new LinkExtractor()
			val wikiPage = WikiPageReader.xmlToWikiPage(XML.loadString(pageSource))
			val links = extractor.extractLinks(wikiPage)
			links
		} map {
			// count each link with one
			(_, 1)
		}

		val surfaceCounts = links
			.groupBy { case (link, _) => (link.text, link.destinationPage) }
			.reduce(count)
			.map { case (link, count) => (link.text, link.destinationPage, count) }

		val contextLinkCounts = links.groupBy( { case (link, _) => (link.sourcePage, link.destinationPage) })
			.reduce(count)
			.map { case (link, count) => (link.sourcePage, link.destinationPage, count) }

		val countsOutput = surfaceCounts.write(surfaceCountsPath, CsvOutputFormat())
		val contextLinkOutput = contextLinkCounts.write(contextLinkCountsPath, CsvOutputFormat())
		(countsOutput, contextLinkOutput)
	}

	/**
	 * Builds the plan who creates the language model for a given entity.
	 */
	def buildWordCountPlan(pageSource: DataSet[String]): ScalaSink[(String, String, Int)] = {
		val wordCount = pageSource.flatMap { pageSource =>
			val (title, text) = WikiPageReader.xmlToPlainText(XML.loadString(pageSource))
			println(text)
			val analyzer = new TextAnalyzer
			val tokens = analyzer.analyze(text).map { token => (title, token) }
			tokens
		} map { case (title, token) =>
			(title, token, 1)
		}
		val languageModel = wordCount.groupBy { case (title, token, _) => (title, token) }
			.reduce { (t1, t2) => (t1._1, t1._2, t1._3 + t2._3) }

		val tokensOutput = languageModel.write(languageModelsPath, CsvOutputFormat())
		tokensOutput
	}

	/**
	 * Helper function to accumulate the counts of a group by result.
	 */
	private def count(l1: (Link, Int), l2: (Link, Int)): (Link, Int) = {
		(l1._1, l1._2 + l2._2)
	}
}
