package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.io.LanguageModelOutputFormat
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.ml.SecondOrderFeatures
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import de.uni_potsdam.hpi.coheel.wiki._
import org.apache.flink.api.common.functions.{RichJoinFunction, RichMapFunction}
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector

import scala.StringBuilder
import scala.collection.mutable
import scala.reflect.ClassTag

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
		val wikiPages = readWikiPages
		buildLinkPlans(wikiPages)
		buildLanguageModelPlan(wikiPages)
		wikiPages.map { wikiPage =>
			(wikiPage.pageTitle, wikiPage.isDisambiguation, wikiPage.isList, wikiPage.isRedirect, wikiPage.ns, if (wikiPage.isNormalPage) "normal" else "special")
		}.writeAsTsv(wikiPagesPath)
	}

	/**
	 * Builds two plans:
	 * <ul>
	 *   <li> the plan which counts how often one document links to another
	 *   <li> the plan which counts how often a link occurs under a certain surface
	 */
	def buildLinkPlans(wikiPages: DataSet[WikiPage]): Unit = {
		val normalPages = wikiPages.filter { !_.isDisambiguation }

		val normalPageLinks = linksFrom(normalPages)
		val allPageLinks    = linksFrom(wikiPages)

		val groupedBySurface = allPageLinks
			.groupBy { link => link.surfaceRepr }

		// counts in how many documents a surface occurs
		val surfaceDocumentCounts = groupedBySurface
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

		val surfaceDestinationCountsUnresolved = groupedBySurface
			.reduceGroup { (groupIt, out: Collector[SurfaceLinkCountsResolving]) =>
				val group = groupIt.toList
				val surface = group.head.surfaceRepr
				group
					.groupBy(_.destination)
					.foreach { case (destination, destGroup) =>
						out.collect(SurfaceLinkCountsResolving(surface, destination, destGroup.size))
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
		val contextLinksUnresolved = linkCounts.join(contextLinkCounts)
			.where     { _.source }
			.equalTo { _.source }
			.map { joinResult => joinResult match {
				case (linkCount, surfaceLinkCount) =>
				ContextLinkResolving(linkCount.source, surfaceLinkCount.destination, surfaceLinkCount.count.toDouble / linkCount.count)
			}
		}

		// save redirects (to - from)
		val redirects = wikiPages
			.filter { wikiPage => wikiPage.isRedirect }
			.map { wikiPage => Redirect(wikiPage.pageTitle, wikiPage.redirect) }

		def iterate[T <: ThingToResolve[T] : TypeInformation : ClassTag](ds: DataSet[T]): DataSet[T] = {
			val resolvedRedirects = ds.leftOuterJoin(redirects)
				.where { _.to }
				.equalTo { _.from }
				.apply(new RichJoinFunction[T, Redirect, T] {
					override def join(contextLink: T, redirect: Redirect): T= {
						if (redirect == null)
							contextLink
						else
							contextLink.updateTo(redirect.to)
					}
				}).name("Resolved-Redirects-From-Iteration")
			resolvedRedirects
		}

		val contextLinks = contextLinksUnresolved.iterate(3)(iterate)
			.groupBy("from", "to")
			.reduce { (cl1, cl2) =>
				cl1.copy(prob = cl1.prob + cl2.prob)
			}
			.map { cl => (cl.from, cl.to, cl.prob) }
			.name("Final-Resolved-Context-Links")

		val surfaceProbs = surfaceDestinationCountsUnresolved.iterate(3)(iterate)
			.groupBy("surface")
			.reduceGroup { (groupIt, out: Collector[(String, String, Double)]) =>
				val surfaceGroup = groupIt.toSeq
				val surface = surfaceGroup.head.surface
				val size = surfaceGroup.map(_.count).sum
				surfaceGroup.groupBy(_.destination)
					.foreach { case (destination, destinationGroup) =>
						out.collect((surface, destination, destinationGroup.map(_.count).sum.toDouble / size))
					}
			}

		contextLinks.writeAsTsv(contextLinkProbsPath)
		surfaceProbs.writeAsTsv(surfaceProbsPath)

		allPageLinks.map { link => (link.fullId, link.surfaceRepr, link.surface, link.source, link.destination) }.writeAsTsv(allLinksPath)
		contextLinksUnresolved.writeAsTsv(contextLinkProbsPath)
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
			val groupedWords = wikiPage.plainText
				.groupBy(identity)
			val model = groupedWords.mapValues { v => v.length }
			LanguageModel(wikiPage.pageTitle, model)
		}.name("Language Model: Document-Word-Prob")

		// count document word counts (in how many documents does a word occur?)
		val documentWordCounts = languageModels
			.flatMap { lm => lm.model.keysIterator.map { word => (word, 1) } }
			.groupBy(0)
			.aggregate(Aggregations.SUM, 1)
			.name("Document Word Counts: Word-DocumentCount")

		plainTexts.writeAsTsv(plainTextsPath)
		languageModels.map { lm =>
			val sb = new StringBuilder()
			sb.append(lm.pageTitle)
			var first = true
			lm.model.foreach { case (word, count) =>
				assert(!word.contains(" "))
				if (first) {
					sb.append(s"\t$word\0$count")
					first = false
				} else {
					sb.append(s" $word\0$count")
				}
			}
			sb.toString()
		}.writeAsText(languageModelsPath, FileSystem.WriteMode.OVERWRITE)
		documentWordCounts.writeAsTsv(documentWordCountsPath)

		languageModels
	}

}
