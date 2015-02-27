package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import org.apache.flink.api.scala._
import de.uni_potsdam.hpi.coheel.wiki._
import scala.collection.mutable

import DataClasses._
import org.apache.log4j.Logger

class WikipediaTrainingProgram extends NoParamCoheelProgram {

	override def getDescription = "Wikipedia Extraction"

	/**
	 * Builds a plan to create the three main data structures CohEEL needs.
	 * <ul>
	 *   <li> How often is each entity mentioned under a certain surface.
	 *   <li> How often does entity A link to entity B?
	 *   <li> How often does each word occur in an entity's text.
	 * @param env Flink execution environment.
	 */
	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val wikiPages = getWikiPages()
		if (!configurationParams.contains(ConfigurationParams.ONLY_WIKIPAGES) && !configurationParams.contains(ConfigurationParams.ONLY_PLAINTEXTS)) {
			buildLinkPlans(wikiPages)
			buildLanguageModelPlan(wikiPages)
		}

		if (configurationParams.contains(ConfigurationParams.ONLY_WIKIPAGES)) {
			wikiPages.map { wikiPage =>
				(wikiPage.pageTitle, wikiPage.isDisambiguation, wikiPage.isList, wikiPage.isRedirect, wikiPage.ns, if (wikiPage.isNormalPage) "normal" else "special")
			}.writeAsTsv(wikiPagesPath)
		}
		if (configurationParams.contains(ConfigurationParams.ONLY_PLAINTEXTS)) {
			wikiPages.map { wikiPage =>
				(
					wikiPage.pageTitle,
					if (wikiPage.plainText.isEmpty) " " else TokenizerHelper.transformToTokenized(wikiPage.plainText),
					if (wikiPage.links.isEmpty) CoheelProgram.LINK_SPLITTER else wikiPage.links.map(_.surfaceRepr).mkString(CoheelProgram.LINK_SPLITTER)
				)
			}.writeAsTsv(plainTextsPath)
		}
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
			.groupBy { link => link.surfaceRepr }
		// counts in how many documents a surface occurs
		val surfaceDocumentCounts = groupedByLinkText
			.reduceGroup { linksWithSameText =>
				var surfaceRepr: String = null
				// Count each link on one source page only once, i.e. if a surface occurs twice on a page
				// it is only counted once.
				val distinctDocuments = mutable.HashSet[String]()
				val list = linksWithSameText.toList
				list.foreach { linkWithText =>
					if (surfaceRepr == null)
						surfaceRepr = linkWithText.surfaceRepr
					distinctDocuments += linkWithText.source
				}
				val count = distinctDocuments.size
				(surfaceRepr, list.head.surface, count)
			}

		// count how often a surface occurs
		val surfaceCounts = groupedByLinkText
			.reduceGroup { group =>
				val links = group.toList
				SurfaceCounts(links.head.surfaceRepr, links.size)
			}
		// count how often a surface occurs with a certain destination
		val surfaceLinkCounts = allPageLinks
			.map { link => SurfaceLinkCounts(link.surfaceRepr, link.destination, 1) }
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
		val words = filterNormalPages(wikiPages) flatMap { wikiPage =>
			val tokens = TokenizerHelper.tokenizeWithCounts(wikiPage.plainText, stemming = false).map { case (token, count) =>
				WordInDocument(wikiPage.pageTitle, token, count)
			}.toIterator
			tokens
		}

		// count the words in a document
		val documentCounts = words.name("Tokenization")
			.groupBy(0)
			.sum(2).name("Document-Counts")
		val wordCounts = words
			.groupBy(1)
			.sum(2).name("Word-Counts")
		val languageModel = documentCounts.join(wordCounts)
			.where { _.document }
			.equalTo { _.document }
			.map { joinResult => joinResult match {
				case (documentCount, wordCount) =>
					(documentCount.document, wordCount.word, wordCount.count.toDouble / documentCount.count)
			}
		}.name("Language Model: Document-Word-Prob")


		// count document word counts (in how many documents does a word occur?)
		val documentWordCounts = words
			.groupBy { word => word.word }
			.reduceGroup { it =>
				var word: String = null
				var size: Int = 0
				it.foreach { wordDocument =>
					if (word == null)
						word = wordDocument.word
					size += 1
				}
				(word, size)
		}.name("Document Word Counts: Word-DocumentCount")

		languageModel.writeAsTsv(languageModelProbsPath)
		documentWordCounts.writeAsTsv(documentWordCountsPath)
	}
}
