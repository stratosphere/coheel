package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.PerformanceTimer
import de.uni_potsdam.hpi.coheel.io.OutputFiles
import org.apache.flink.api.common.ProgramDescription
import org.apache.flink.api.scala._
import de.uni_potsdam.hpi.coheel.wiki._
import scala.collection.mutable

import OutputFiles._
import DataClasses._
import org.apache.log4j.Logger

class WikipediaTrainingProgram extends CoheelProgram with ProgramDescription {

	@transient val log: Logger = Logger.getLogger(this.getClass)

	override def getDescription = "Training the model parameters for CohEEL."

	/**
	 * Builds a plan to create the three main data structures CohEEL needs.
	 * <ul>
	 *   <li> How often is each entity mentioned under a certain surface.
	 *   <li> How often does entity A link to entity B?
	 *   <li> How often does each word occur in an entity's text.
	 * @param env Flink execution environment.
	 */
	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val wikiPages = ProgramHelper.getWikiPages(env)
		buildLinkPlans(wikiPages)
		buildLanguageModelPlan(wikiPages)

//		wikiPages.map { wikiPage =>
//			(wikiPage.pageTitle, wikiPage.plainText)
//		}.writeAsTsv(textDumpsPath)
	}

	/**
	 * Builds two plans:
	 * <ul>
	 *   <li> the plan who counts how often one document links to another
	 *   <li> the plan who counts how often a link occurs under a certain surface
	 */
	def buildLinkPlans(wikiPages: DataSet[WikiPage]): Unit = {
		val normalPages = wikiPages.filter { !_.isDisambiguation }

		val normalPageLinks = linksFrom(normalPages)
		val allPageLinks    = linksFrom(wikiPages)

		val groupedByLinkText = allPageLinks
			.groupBy { link => link.surface }
		// counts in how many documents a surface occurs
		val surfaceDocumentCounts = groupedByLinkText
			.reduceGroup { linksWithSameText =>
				var text: String = null
				// Count each link on one source page only once, i.e. if a surface occurs twice on a page
				// it is only counted once.
				val distinctDocuments = mutable.HashSet[String]()
				linksWithSameText.foreach { linkWithText =>
					if (text == null)
						text = linkWithText.surface
					distinctDocuments += linkWithText.source
				}
				val count = distinctDocuments.size
				(text, count)
			}

		// count how often a surface occurs
		val surfaceCounts = groupedByLinkText
			.reduceGroup { group =>
				val links = group.toList
				SurfaceCounts(links.head.surface, links.size)
			}
		// count how often a surface occurs with a certain destination
		val surfaceLinkCounts = allPageLinks
			.map { link => SurfaceLinkCounts(link. surface, link.destination, 1) }
			.groupBy(0, 1)
			.sum(2).name("Surface-LinkTo-Counts")
		// join them together and calculate the probabilities
		val surfaceProbabilities = surfaceCounts.join(surfaceLinkCounts)
			.where { _.surface }
			.equalTo { _.surface }
			.map { joinResult => joinResult match {
				case (surfaceCount, surfaceLinkCount) =>
					(surfaceCount.surface, surfaceLinkCount.destination,
					 surfaceLinkCount.count / surfaceCount.count.toDouble)
			}
		}

		// calculate context link counts only for non-disambiguation pages
		val linkCounts = normalPageLinks
			.map { link => LinkCounts(link.source, 1) }
			.groupBy(0)
			.sum(1)
		val contextLinkCounts = normalPageLinks
			.map { link => ContextLinkCounts(link.source, link.destination, 1) }
			.groupBy(0, 1)
			.sum(2)
//			.reduceGroup { group =>
//				val links = group.toList
//				ContextLinkCounts(links.head.source, links.head.destination, links.size)
//			}
		val contextLinkProbabilities = linkCounts.join(contextLinkCounts)
			.where     { _.source }
			.equalTo { _.source }
			.map { joinResult => joinResult match {
				case (linkCount, surfaceLinkCount) =>
				(linkCount.source, surfaceLinkCount.destination, surfaceLinkCount.count.toDouble / linkCount.count)
			}
		}

		// save redirects (to - from)
		val redirects = wikiPages
			.filter { wikiPage => wikiPage.isRedirect }
			.map { wikiPage => (wikiPage.pageTitle, wikiPage.redirect) }


		surfaceProbabilities.writeAsTsv(surfaceProbsPath)
		contextLinkProbabilities.writeAsTsv(contextLinkProbsPath)
		redirects.writeAsTsv(redirectPath)
		surfaceDocumentCounts.writeAsTsv(surfaceDocumentCountsPath)
	}

	def linksFrom(pages: DataSet[WikiPage]): DataSet[Link] = {
		pages.flatMap { wikiPage =>
			wikiPage.links.toIterator
		}
	}

	/**
	 * Builds the plan who creates the language model for a given entity.
	 */
	def buildLanguageModelPlan(wikiPages: DataSet[WikiPage]): Unit = {
		val words = ProgramHelper.filterNormalPages(wikiPages) flatMap { wikiPage =>
			val tokens = TokenizerHelper.tokenize(wikiPage.plainText).map { token =>
				Word(wikiPage.pageTitle, token)
			}.toIterator
			tokens
		}

		// count the words in a document
		val documentCounts = words.name("Tokenization")
			.map { word => DocumentCounts(word.document, 1) }
			.groupBy(0)
			.sum(1).name("Document-Counts")
		val wordCounts = words
			.map { word => WordCounts(word, 1) }
			.groupBy(0)
			.sum(1).name("Document-Counts").name("Word-Counts")
		val languageModel = documentCounts.join(wordCounts)
			.where { _.document }
			.equalTo { _.word.document }
			.map { joinResult => joinResult match {
				case (documentCount, wordCount) =>
					(documentCount.document, wordCount.word.word, wordCount.count.toDouble / documentCount.count)
			}
		}.name("Language Model: Document-Word-Prob")


		// count document word counts (in how many documents does a word occur?)
		val documentWordCounts = words
			.groupBy { word => word.word }
			.reduceGroup { it =>
				var word: String = null
				val distinctDocuments = mutable.HashSet[String]()
				it.foreach { wordDocument =>
					if (word == null)
						word = wordDocument.word
					distinctDocuments += wordDocument.document
				}
				(word, distinctDocuments.size)
		}.name("Document Word Counts: Word-DocumentCount")

		languageModel.writeAsTsv(languageModelProbsPath)
		documentWordCounts.writeAsTsv(documentWordCountsPath)
	}
}
