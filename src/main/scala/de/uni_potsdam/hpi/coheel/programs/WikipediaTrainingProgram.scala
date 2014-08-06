package de.uni_potsdam.hpi.coheel.programs

import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import eu.stratosphere.api.common.{Program, ProgramDescription, Plan}
import de.uni_potsdam.hpi.coheel.wiki._
import scala.io.Source
import de.uni_potsdam.hpi.coheel.wiki.Link
import DataSetNaming.toDataSetWithName

import OutputFiles._
import org.slf4s.Logging
import java.io.File


class WikipediaTrainingProgram(dumpFile: File = new File("src/test/resources/test.wikirun"))
	extends Program with ProgramDescription with Logging {

	override def getDescription = "Training the model parameters for CohEEL."

	// input files, file with the names of the test wikipedia articles
	lazy val wikipediaFilesPath = s"file://${dumpFile.getAbsolutePath}"

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
		val wikiPages = input.map { file =>
			log.info(file)
			val pageSource = Source.fromFile(s"${dumpFile.getAbsoluteFile.getParent}/$file").mkString
			pageSource
		}.flatMap { pageSource =>
			if (pageSource.startsWith("#")) {
				List()
			} else {
				val wikiPages = WikiPageReader.xmlToWikiPages(pageSource)

				wikiPages.filter { page => page.ns == 0 }
			}
		}
		val plans = buildLinkPlans(wikiPages)
		val languageModelPlans = buildLanguageModelPlan(wikiPages)

		val textDumps = wikiPages.map { wikiPage =>
			(wikiPage.pageTitle, wikiPage.text)
		}.write(textDumpsPath, textFormat)

		val plan = new ScalaPlan(
			 textDumps :: languageModelPlans ::: plans)

		plan
	}

	/**
	 * Builds two plans:
	 * <ul>
	 *   <li> the plan who counts how often one document links to another
	 *   <li> the plan who counts how often a link occurs under a certain surface
	 */
	def buildLinkPlans(wikiPages: DataSet[CoheelWikiPage]): List[ScalaSink[_]] = {
		val normalPages = wikiPages.filter { !_.isDisambiguation }

		val normalPageLinks = linksFrom(normalPages)
		val allPageLinks    = linksFrom(wikiPages)

		var i = 0
		val groupedByLinkText = allPageLinks
			.groupBy { link => link.text }
		// counts in how many documents a surface occurs
		val surfaceDocumentCounts = groupedByLinkText
			.reduceGroup { linksWithSameText =>
				val asList = linksWithSameText.toList
				val text = asList(0).text

				// Count each link on one source page only once, i. e. if a surface occurs twice on a page
				// it is only counted once.
				// Note: These are scala functions, no Flink functions.
				//       Hoping that the list of links with a certain surface is small enough to be handled on
				//       one machine.
				val count = asList
					.groupBy { link => link.source  }
					.size

				if (i % 1000000 == 0)
					log.info(s"Surface document counts: $i ")
				i += 1
				(text, count)
			}


		var j = 0
		// count how often a surface occurs
		val surfaceCounts = groupedByLinkText
			.count()
		// count how often a surface occurs with a certain destination
		val surfaceLinkCounts = allPageLinks
			.groupBy { link => (link.text, link.destination) }
			.count()
			.name("Surface-LinkTo-Counts")
		// join them together and calculate the probabilities
		val surfaceProbabilities = surfaceCounts.join(surfaceLinkCounts)
			.where     { case (link, _) => link.text }
			.isEqualTo { case (link, _) => link.text }
			.map { case (surfaceCount, surfaceLinkCount) =>
				val link = surfaceLinkCount._1
				if (j % 1000000 == 0)
					log.info(s"Surface probabilities: $j ")
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
				if (k % 1000000 == 0)
					log.info(s"Context link probabilities: $k ")
				k += 1
				(link.source, link.destination, surfaceLinkCount._2.toDouble / linkCount._2.toDouble)
			}

		// save redirects (to - from)
		val redirects = wikiPages
			.filter { wikiPage => wikiPage.isRedirect }
			.map { wikiPage => (wikiPage.pageTitle, wikiPage.redirectTitle) }


		val surfaceProbOutput = surfaceProbabilities.write(surfaceProbsPath, probOutputFormat)
		val contextLinkOutput = contextLinkProbabilities.write(contextLinkProbsPath, probOutputFormat)
		val redirectOutput    = redirects.write(redirectPath, textFormat)
		val surfaceDocumentsOutput = surfaceDocumentCounts.write(surfaceDocumentPath, surfaceDocumentFormat)
		List(surfaceProbOutput, contextLinkOutput, redirectOutput, surfaceDocumentsOutput)
	}

	def linksFrom(pages: DataSet[CoheelWikiPage]): DataSet[Link] = {
		pages.flatMap { wikiPage =>
			// extract all links
			val extractor = new LinkExtractor()
			try {
				extractor.extractLinks(wikiPage)
			} catch {
				case e: Throwable =>
					println(s"Error in ${wikiPage.pageTitle}")
					List()
			}
		}
	}

	/**
	 * Builds the plan who creates the language model for a given entity.
	 */
	def buildLanguageModelPlan(wikiPages: DataSet[CoheelWikiPage]): List[ScalaSink[_]] = {
		// Helper case class to avoid passing tuples around
		case class Word(document: String, word: String)
		val words = wikiPages.filter { wikiPage =>
			!wikiPage.isDisambiguation && !wikiPage.isRedirect && !wikiPage.isList
		} flatMap { wikiPage =>
			val (doc, text) = WikiPageReader.wikiPageToText(wikiPage)
			val tokens = TextAnalyzer.tokenize(text).map { token => Word(doc, token) }
			tokens
		}

		var i = 0

		// count the words in a document
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
				if (i % 10000000 == 0)
					log.info(s"Language Models: $i ")
				i += 1
				(word.document, word.word, wordCount._2.toDouble / documentCount._2.toDouble)
			}.name("Language Model: Document-Word-Prob")

		// count document frequencies
		val documentFrequencies = words
			.groupBy { word => word.word }
			.reduceGroup { it =>
			val docList = it.toList
			(docList(0).word, docList.groupBy { word => word.document }.size)
		}.name("Document Frequencies: Word-DocFrequency")
		val languageModelsOutput = languageModel.write(languageModelsPath, probOutputFormat)
		val documentFrequencyOutput = documentFrequencies.write(documentFrequencyPath, surfaceDocumentFormat)
		List(languageModelsOutput, documentFrequencyOutput)
	}
}
