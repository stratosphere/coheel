package de.hpi.uni_potsdam.coheel_stratosphere

import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import eu.stratosphere.api.common.{Program, ProgramDescription, Plan}
import de.hpi.uni_potsdam.coheel_stratosphere.wiki._
import scala.xml.XML
import scala.io.Source
import eu.stratosphere.api.java.ExecutionEnvironment
import de.hpi.uni_potsdam.coheel_stratosphere.wiki.Link


class WikipediaTrainingTask(path: String = "src/test/resources/wikipedia_files.txt") extends Program with ProgramDescription {

	override def getDescription = "Training the model parameters for CohEEL."

	val outputFormat = CsvOutputFormat[(String, String, Int)]("\n", "\t")
	val probOutputFormat = CsvOutputFormat[(String, String, Double)]("\n", "\t")
	lazy val currentPath = System.getProperty("user.dir")
	// input files, file with the names of the test wikipedia articles
	lazy val wikipediaFilesPath = s"file://$currentPath/$path"
	// outputs files
	lazy val surfaceProbsPath      = s"file://$currentPath/testoutput/surface-probs"
	lazy val contextLinkProbsPath  = s"file://$currentPath/testoutput/context-link-probs"
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
		}.flatMap { pageSource =>
			// extract all links
			val extractor = new LinkExtractor()
			val wikiPages = WikiPageReader.xmlToWikiPages(XML.loadString(pageSource))
			wikiPages.toList
		}

		val (linkCountPlan, linkContextCountPlan) = buildLinkPlans(pageSource)
		val wordCountPlan = buildWordCountPlan(pageSource)
		val plan = new ScalaPlan(Seq(linkCountPlan, wordCountPlan, linkContextCountPlan))

		plan
	}

	/**
	 * Builds two plans:
	 * <ul>
	 *   <li> the plan who counts how often one document links to another
	 *   <li> the plan who counts how often a link occurs under a certain surface
	 */
	def buildLinkPlans(wikiPages: DataSet[CoheelWikiPage]):
		(ScalaSink[(String, String, Double)], ScalaSink[(String, String, Double)]) = {
		val disambiguationPages = wikiPages.filter { _.isDisambiguation }
		val normalPages = wikiPages.filter { !_.isDisambiguation }

		val disambiguationPageLinks = linksFrom(disambiguationPages)
		val normalPageLinks         = linksFrom(normalPages)
		// Note:
		// It seems to be a bug in Stratosphere, that you cannot use the same union
		// twice for a upcoming join
		// so we create two unions here as a workaround, until this is fixed
		val allPages1 = disambiguationPageLinks.union(normalPageLinks)
		val allPages2 = disambiguationPageLinks.union(normalPageLinks)

		// count how often a surface occurs
		val surfaceCounts = allPages1
			.groupBy { link => link.text }
			.count()
		// count how often a surface occurs with a certain destination
		val surfaceLinkCounts = allPages2
			.groupBy { link => (link.text, link.destination) }
			.count()
		// join them together and calculate the probabilities
		val surfaceProbabilities = surfaceCounts.join(surfaceLinkCounts)
			.where     { case (link, _) => link.text }
			.isEqualTo { case (link, _) => link.text }
			.map { case (surfaceCount, surfaceLinkCount) =>
				val link = surfaceLinkCount._1
				(link.text, link.destination, surfaceLinkCount._2.toDouble / surfaceCount._2.toDouble)
			}

		// calculate context link counts only for non-disambiguation pages
		val linkCounts = normalPageLinks
			.groupBy { link => link.source }
			.count()
		val contextLinkCounts = normalPageLinks
			.groupBy { link => (link.source, link.destination) }
			.count()
		val contextLinkProbabilities = linkCounts.join(contextLinkCounts)
			.where     { case (link, _) => link.source }
			.isEqualTo { case (link, _) => link.source }
			.map { case (linkCount, surfaceLinkCount) =>
				val link = surfaceLinkCount._1
				(link.source, link.destination, surfaceLinkCount._2.toDouble / linkCount._2.toDouble)
			}


		val surfaceProbOutput = surfaceProbabilities.write(surfaceProbsPath, probOutputFormat)
		val contextLinkOutput = contextLinkProbabilities.write(contextLinkProbsPath, probOutputFormat)
		(surfaceProbOutput, contextLinkOutput)
	}

	def linksFrom(pages: DataSet[CoheelWikiPage]): DataSet[Link] = {
		pages.flatMap { wikiPage =>
			// extract all links
			val extractor = new LinkExtractor()
			extractor.extractLinks(wikiPage)
		}
	}

	/**
	 * Builds the plan who creates the language model for a given entity.
	 */
	def buildWordCountPlan(wikiPages: DataSet[CoheelWikiPage]): ScalaSink[(String, String, Int)] = {
		val wordCount = wikiPages.filter { wikiPage =>
			!wikiPage.isDisambiguation && !wikiPage.isRedirect && !wikiPage.isList
		} flatMap { wikiPage =>
			val (title, text) = WikiPageReader.wikiPageToText(wikiPage)
			val analyzer = new TextAnalyzer
			val tokens = analyzer.tokenize(text).map { token => (title, token) }
			tokens
		} map { case (title, token) =>
			(title, token, 1)
		}
		val languageModel = wordCount.groupBy { case (title, token, _) => (title, token) }
			.reduce { (t1, t2) => (t1._1, t1._2, t1._3 + t2._3) }

		val tokensOutput = languageModel.write(languageModelsPath, outputFormat)
		tokensOutput
	}
}
