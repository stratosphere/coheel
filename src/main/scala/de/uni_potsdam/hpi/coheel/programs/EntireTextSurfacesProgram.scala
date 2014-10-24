package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.datastructures.TrieBuilder
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper
import org.apache.flink.api.common.{Plan, ProgramDescription}
import OutputFiles._
import org.apache.flink.api.scala._
import org.apache.log4j.Logger

import scala.collection.mutable

class EntireTextSurfacesProgram extends CoheelProgram with ProgramDescription {

	val log = Logger.getLogger(getClass)
	// prepare the trie
	TrieBuilder.buildFullTrie()

	override def getDescription = "Counting how often a surface occurs, in the entire text. This approach uses a trie."

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val wikiPages = ProgramHelper.getWikiPages(env)

		var c = 0

		case class EntireTextSurfaces(pageTitle: String, surface: String)
		// which surfaces occur in which documents
		val entireTextSurfaces = ProgramHelper.filterNormalPages(wikiPages).flatMap { wikiPage =>
			if (c % 200000 == 0)
				log.info(f"$c%8s/11023933")
			c += 1
			val tokens = TokenizerHelper.tokenize(wikiPage.plainText).toArray

			val resultSurfaces = mutable.HashSet[String]()

			// each word and its following words must be checked, if it is a surface
			for (i <- 0 until tokens.size) {
				resultSurfaces ++= TrieBuilder.fullTrie.slidingContains(tokens, i).map {
					containment => containment.mkString(" ")
				}
			}
			resultSurfaces.map { surface => EntireTextSurfaces(wikiPage.pageTitle, surface) }.toIterator
		}.name("Entire-Text-Surfaces-Along-With-Document")

		val surfaceDocumentCounts = env.readTextFile(surfaceDocumentCountsPath)

		case class EntireTextSurfaceCounts(surface: String, count: Int)
		val entireTextSurfaceCounts = entireTextSurfaces
			.groupBy { _.surface }
			.reduceGroup { group =>
				val surfaces = group.toList
				EntireTextSurfaceCounts(surfaces.head.surface, surfaces.size)
			}
			.name("Entire-Text-Surface-Counts")


		case class SurfaceAsLinkCount(surface: String, count: Int)
		val surfaceLinkProbs = surfaceDocumentCounts.map { line =>

			val split = line.split('\t')
			// not clear, why lines without a count occur, but they do
			try {
				if (split.size < 2)
					SurfaceAsLinkCount(split(0), 0)
				else {
					val (surface, count) = (split(0), split(1).toInt)
					SurfaceAsLinkCount(TokenizerHelper.tokenize(surface).mkString(" "), count)
				}
			} catch {
				case e: NumberFormatException =>
					SurfaceAsLinkCount(split(0), 0)
			}
		}.join(entireTextSurfaceCounts)
			.where { _.surface }
			.equalTo { _.surface }
			.map { joinResult => joinResult match {
				case (surfaceAsLinkCount, entireTextSurfaceCount) =>
					(surfaceAsLinkCount.surface, entireTextSurfaceCount.count,
						surfaceAsLinkCount.count.toDouble / entireTextSurfaceCount.count.toDouble)
			}
		}

		entireTextSurfaces.writeAsTsv(entireTextSurfacesPath)
		surfaceLinkProbs.writeAsTsv(surfaceLinkProbsPath)
	}
}
