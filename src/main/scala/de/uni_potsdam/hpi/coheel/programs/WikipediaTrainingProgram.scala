package de.uni_potsdam.hpi.coheel.programs

import org.apache.flink.api.common.ProgramDescription
import org.apache.flink.api.scala._
import de.uni_potsdam.hpi.coheel.wiki._

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

//		val textDumps = wikiPages.map { wikiPage =>
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
				val asList = linksWithSameText.toList
				val text = asList(0).surface

				// Count each link on one source page only once, i.e. if a surface occurs twice on a page
				// it is only counted once.
				// Note: These are scala functions, no Flink functions.
				//       Hoping that the list of links with a certain surface is small enough to be handled on
				//       one slave.
				val count = asList
					.groupBy { link => link.source  }
					.size

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
			.groupBy { link => (link.surface, link.destination) }
			.reduceGroup { group =>
				val links = group.toList
				SurfaceLinkCounts(links.head.surface, links.head.destination, links.size)
			}
			.name("Surface-LinkTo-Counts")
		// join them together and calculate the probabilities
		val surfaceProbabilities = surfaceCounts.join(surfaceLinkCounts)
			.where { _.surface }
			.equalTo { _.surface }
			.map { joinResult => joinResult match {
				case (surfaceCount, surfaceLinkCount) =>
					(surfaceLinkCount.surface, surfaceLinkCount.destination,
					 surfaceLinkCount.count / surfaceCount.count.toDouble)
			}
		}

		// calculate context link counts only for non-disambiguation pages
		val linkCounts = normalPageLinks
			.groupBy { link => link.source }
			.reduceGroup { group =>
				val links = group.toList
				LinkCounts(links.head.source, links.size)
			}
		val contextLinkCounts = normalPageLinks
			.groupBy { link => (link.source, link.destination) }
			.reduceGroup { group =>
				val links = group.toList
				ContextLinkCounts(links.head.source, links.head.destination, links.size)
			}
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


		val surfaceProbOutput      = surfaceProbabilities.writeAsTsv(surfaceProbsPath)
		val contextLinkOutput      = contextLinkProbabilities.writeAsTsv(contextLinkProbsPath)
		val redirectOutput         = redirects.writeAsTsv(redirectPath)
		val surfaceDocumentsOutput = surfaceDocumentCounts.writeAsTsv(surfaceDocumentCountsPath)
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

//		var i = 0

		// count the words in a document
		val documentCounts = words
			.groupBy { word => word.document }
			.reduceGroup { group =>
				val words = group.toList
				DocumentCounts(words.head.document, words.size)
			}.name("Document-Counts")
		val wordCounts = words
			.groupBy { word => word }
			.reduceGroup { group =>
				val words = group.toList
				WordCounts(words.head, words.size)
			}.name("Word-Counts")
		val languageModel = documentCounts.join(wordCounts)
			.where { _.document }
			.equalTo { _.word.document }
			.map { joinResult => joinResult match {
				case (documentCount, wordCount) =>
//					if (i % 10000000 == 0)
//						log.info(s"Language Models: $i ")
//					i += 1
					(documentCount.document, wordCount.word, wordCount.count.toDouble / documentCount.count)
			}
		}.name("Language Model: Document-Word-Prob")


		// count document word counts (in how many documents does a word occur?)
		val documentWordCounts = words
			.groupBy { word => word.word }
			.reduceGroup { it =>
				val docList = it.toList
				(docList(0).word, docList.groupBy { word => word.document }.size)
		}.name("Document Word Counts: Word-DocumentCount")

		val languageModelsOutput = languageModel.writeAsTsv(languageModelProbsPath)
		val documentWordCountsOutput = documentWordCounts.writeAsTsv(documentWordCountsPath)
	}
}
