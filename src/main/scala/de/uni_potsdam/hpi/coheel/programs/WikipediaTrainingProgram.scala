package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.io.LanguageModelOutputFormat
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.ml.SecondOrderFeatures
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import de.uni_potsdam.hpi.coheel.wiki._
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector

import scala.collection.mutable

class WikipediaTrainingProgram extends NoParamCoheelProgram with Serializable {

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
		val wikiPages = getWikiPages
		if (!configurationParams.contains(ConfigurationParams.ONLY_WIKIPAGES)) {
			buildLinkPlans(wikiPages)
			buildLanguageModelPlan(wikiPages)
		} else {
			wikiPages.map { wikiPage =>
				(wikiPage.pageTitle, wikiPage.isDisambiguation, wikiPage.isList, wikiPage.isRedirect, wikiPage.ns, if (wikiPage.isNormalPage) "normal" else "special")
			}.writeAsTsv(wikiPagesPath)
		}
	}

	/**
	 * Builds two plans:
	 * <ul>
	 *   <li> the plan who counts how often one document links to another
	 *   <li> the plan who counts how often a link occurs under a certain surface
	 */
	def buildLinkPlans(wikiPages: DataSet[WikiPage]):
		(DataSet[(String, String, Double)], DataSet[(String, String)]) = {
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
				// for debugging purposes, also output one variant of the actual surface (untokenized, unstemmed)
				(surfaceRepr, list.minBy(_.surface).surface, count)
			}

		// count how often a surface occurs
		val surfaceCounts = groupedByLinkText
			.reduceGroup { group =>
				val links = group.toList
				SurfaceCounts(links.head.surfaceRepr, links.size)
			}
		val surfaceCountHistogram = surfaceCounts.map { surfaceCount => (surfaceCount.count, 1) }.groupBy(0).sum(1)

		// count how often a surface occurs with a certain destination
		val surfaceLinkCounts = allPageLinks
			.map { link => SurfaceLinkCounts(link.surfaceRepr, link.destination, 1) }
			.groupBy(0, 1)
			.sum(2).name("Surface-LinkTo-Counts")
		// join them together and calculate the probabilities
		val surfaceProbs = surfaceCounts.join(surfaceLinkCounts)
			.where { _.surfaceRepr }
			.equalTo { _.surface }
			.map { joinResult => joinResult match {
				case (surfaceCount, surfaceLinkCount) =>
					(surfaceCount.surfaceRepr, surfaceLinkCount.destination,
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

		allPageLinks.map { link => (link.fullId, link.surfaceRepr, link.surface, link.source, link.destination) }.writeAsTsv(allLinksPath)
		surfaceCountHistogram.writeAsTsv(surfaceCountHistogramPath)
		surfaceProbs.writeAsTsv(surfaceProbsPath)
		contextLinkProbabilities.writeAsTsv(contextLinkProbsPath)
		redirects.writeAsTsv(redirectPath)
		surfaceDocumentCounts.writeAsTsv(surfaceDocumentCountsPath)

		(surfaceProbs, redirects)
	}

	def linksFrom(pages: DataSet[WikiPage]): DataSet[Link] = {
		pages.flatMap { wikiPage =>
			wikiPage.links.toIterator
		}
	}

	/**
	 * Builds the plan who creates the language model for a given entity.
	 */
	def buildLanguageModelPlan(wikiPages: DataSet[WikiPage]): DataSet[DataClasses.LanguageModel] = {
		val plainTexts = wikiPages.map { wikiPage =>
			val plainText =  if (wikiPage.plainText.isEmpty)
				" "
			else
				wikiPage.plainText.mkString(" ")

			val links = if (wikiPage.links.isEmpty)
				CoheelProgram.LINK_SPLITTER
			else
				wikiPage.links.map(_.surfaceRepr).mkString(CoheelProgram.LINK_SPLITTER)

			(wikiPage.pageTitle, plainText, links)
		}.name("Plain Texts with Links: Title-Text-Links")

		val languageModels = wikiPages.map { wikiPage =>
			val wordsInDoc = wikiPage.plainText.length
			val groupedWords = wikiPage.plainText
				.groupBy(identity)
			val groupCount = groupedWords.size
			val model = groupedWords.mapValues { v => (v.length + 1).toDouble / (wordsInDoc + groupCount) }
			LanguageModel(wikiPage.pageTitle, model)
		}.name("Language Model: Document-Word-Prob")

		// count document word counts (in how many documents does a word occur?)
		val documentWordCounts = languageModels
			.flatMap { lm => lm.model.keysIterator.map { word => (word, 1) } }
			.groupBy(0)
			.aggregate(Aggregations.SUM, 1)
			.name("Document Word Counts: Word-DocumentCount")

		plainTexts.writeAsTsv(plainTextsPath)
		languageModels.write(new LanguageModelOutputFormat, languageModelsPath, FileSystem.WriteMode.OVERWRITE)
		documentWordCounts.writeAsTsv(documentWordCountsPath)

		languageModels
	}

	/**
	 * @param candidatesIt All link candidates with scores (all LinkWithScore's have the same id).
	 */
	def applySecondOrderCoheelFunctions(candidatesIt: Iterator[LinkWithScores],
	                                    out: Collector[(String, String, String, String, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Boolean)]): Unit = {
		val allCandidates = candidatesIt.toSeq
		val promOrder = allCandidates.sortBy(-_.promScore)
		val contextOrder = allCandidates.sortBy(-_.contextScore)
		if (allCandidates.size > 1) {
			val promRank       = SecondOrderFeatures.rank.apply(promOrder)(_.promScore)
			val promDeltaTops  = SecondOrderFeatures.deltaTop.apply(promOrder)(_.promScore)
			val promDeltaSuccs = SecondOrderFeatures.deltaSucc.apply(promOrder)(_.promScore)
			val contextRank       = SecondOrderFeatures.rank.apply(contextOrder)(_.contextScore)
			val contextDeltaTops  = SecondOrderFeatures.deltaTop.apply(contextOrder)(_.contextScore)
			val contextDeltaSuccs = SecondOrderFeatures.deltaSucc.apply(contextOrder)(_.contextScore)

			promOrder.zipWithIndex.foreach { case (candidate, i) =>
				val positiveInstance = candidate.destination == candidate.candidateEntity
				import candidate._
				out.collect((fullId, surfaceRepr, source, candidateEntity, np, vp, promScore, promRank(i), promDeltaTops(i), promDeltaSuccs(i),
					contextScore, contextRank(i), contextDeltaTops(i), contextDeltaSuccs(i), positiveInstance))
			}
		}
	}
}
