package de.uni_potsdam.hpi.coheel.plans

import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import eu.stratosphere.api.common.{Program, ProgramDescription, Plan}
import de.uni_potsdam.hpi.coheel.wiki._
import scala.xml.XML
import scala.io.Source
import de.uni_potsdam.hpi.coheel.wiki.Link

import OutputFiles._
import org.slf4s.Logging


class WikipediaTrainingPlan(path: String = "src/test/resources/test.wikirun")
	extends Program with ProgramDescription with Logging {

	override def getDescription = "Training the model parameters for CohEEL."

	// input files, file with the names of the test wikipedia articles
	lazy val wikipediaFilesPath = s"file://$currentPath/$path"

	lazy val PRINT_EVERY = 100000

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
		var i = 0
		val pageSource = input.map { file =>
			val pageSource = Source.fromFile(s"src/test/resources/$file").mkString
			pageSource
		}.flatMap { pageSource =>
			val wikiPages = WikiPageReader.xmlToWikiPages(XML.loadString(pageSource))
			print(s"Wikifying $i ... ")
			val r = wikiPages.toList
			println("Done.")
			r
		}
		val plans = buildLinkPlans(pageSource)
		val languageModelPlan = buildLanguageModelPlan(pageSource)

		val plan = new ScalaPlan(
			languageModelPlan :: plans)

		plan
	}

	/**
	 * Builds two plans:
	 * <ul>
	 *   <li> the plan who counts how often one document links to another
	 *   <li> the plan who counts how often a link occurs under a certain surface
	 */
	def buildLinkPlans(wikiPages: DataSet[CoheelWikiPage]): List[ScalaSink[_]] = {
		val disambiguationPages = wikiPages.filter { _.isDisambiguation }
		val normalPages = wikiPages.filter { !_.isDisambiguation }

		val disambiguationPageLinks = linksFrom(disambiguationPages)
		val normalPageLinks         = linksFrom(normalPages)
		// Note:
		// It seems to be a bug in Stratosphere, that you cannot use the same union
		// twice for an upcoming join
		// so we create two unions here as a workaround, until this is fixed
		val allPages1 = disambiguationPageLinks.union(normalPageLinks)
		val allPages2 = disambiguationPageLinks.union(normalPageLinks)
		val allPages3 = disambiguationPageLinks.union(normalPageLinks)

		var i = 0
		// counts in how many documents a surface occurs
		val surfaceDocumentCounts = allPages3
			.groupBy { link => link.text }
			.reduceGroup { linksWithSameText =>
				val asList = linksWithSameText.toList
				val text = asList(0).text

				// Note: these are scala functions, no Stratosphere functions
				val count = asList
					.groupBy { link => link.source  }
					.size

				if (i % PRINT_EVERY == 0)
					println(s"Surface document counts: $i ")
				i += 1
				(text, count)
			}


		var j = 0
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
				if (j % PRINT_EVERY == 0)
					println(s"Surface probabilities: $j ")
				j += 1
				(link.text, link.destination, surfaceLinkCount._2.toDouble / surfaceCount._2.toDouble)
			}

		var k = 0
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
				if (k % PRINT_EVERY == 0)
					println(s"Context link probabilities: $k ")
				k += 1
				(link.source, link.destination, surfaceLinkCount._2.toDouble / linkCount._2.toDouble)
			}

		// save redirects (to - from)
		val redirects = wikiPages
			.filter { wikiPage => wikiPage.isRedirect }
			.map { wikiPage => (wikiPage.pageTitle, wikiPage.redirectTitle) }


		val surfaceProbOutput = surfaceProbabilities.write(surfaceProbsPath, probOutputFormat)
		val contextLinkOutput = contextLinkProbabilities.write(contextLinkProbsPath, probOutputFormat)
		val redirectOutput    = redirects.write(redirectPath, redirectFormat)
		val surfaceDocumentsOutput = surfaceDocumentCounts.write(surfaceDocumentPath, surfaceDocumentFormat)
		List(surfaceProbOutput, contextLinkOutput, redirectOutput, surfaceDocumentsOutput)
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
	def buildLanguageModelPlan(wikiPages: DataSet[CoheelWikiPage]): ScalaSink[_] = {
		// Helper case class to avoid passing tuples around
		case class Word(document: String, word: String)
		val words = wikiPages.filter { wikiPage =>
			!wikiPage.isDisambiguation && !wikiPage.isRedirect && !wikiPage.isList
		} flatMap { wikiPage =>
			val (title, text) = WikiPageReader.wikiPageToText(wikiPage)
			// TODO: Refactor this to outer method and see if it still works
			val analyzer = new TextAnalyzer
			val tokens = analyzer.tokenize(text).map { token => Word(title, token) }
			tokens
		}

		var i = 0
		val documentCounts = words
			.groupBy { word => word.document }
			.count()
		val wordCounts = words
			.groupBy { word => word }
			.count()
		val languageModel = documentCounts.join(wordCounts)
			.where { case (word, _) => word.document }
			.isEqualTo { case (word, _) => word.document }
			.map { case (documentCount, wordCount) =>
				val word = wordCount._1
				if (i % PRINT_EVERY == 0)
					println(s"Language Models: $i ")
				i += 1
				(word.document, word.word, wordCount._2.toDouble / documentCount._2.toDouble)
			}

		val tokensOutput = languageModel.write(languageModelsPath, probOutputFormat)
		tokensOutput
	}
}
