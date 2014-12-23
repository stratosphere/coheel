package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.datastructures.{Trie, TrieBuilder}
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.programs.DataClasses.{EntireTextSurfaces, SurfaceAsLinkCount, EntireTextSurfaceCounts}
import de.uni_potsdam.hpi.coheel.wiki.{WikiPage, TokenizerHelper}
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichCrossFunction}
import org.apache.flink.api.common.{Plan, ProgramDescription}
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import org.apache.log4j.Logger

import scala.collection.mutable

class EntireTextSurfacesProgram extends CoheelProgram {

	@transient val log = Logger.getLogger(getClass)
	// prepare the trie
//	TrieBuilder.buildFullTrie()

	override def getDescription = "Counting how often a surface occurs, in the entire text. This approach uses a trie."

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val wikiPages = ProgramHelper.filterNormalPages(ProgramHelper.getWikiPages(env))
		val surfaces = env.readTextFile(surfaceProbsPath).flatMap { line =>
			val split = line.split('\t')
			if (split.size == 3)
				Some(split(0))
			else
				None
		}

		val entireTextSurfaces = surfaces.coGroup(wikiPages)
			.where { surface => 1 }
			.equalTo { wikiPage => 1}
			.flatMap(
				new RichFlatMapFunction[(Array[String], Array[WikiPage]), EntireTextSurfaces] {
					override def flatMap(crossProduct: (Array[String], Array[WikiPage]), out: Collector[EntireTextSurfaces]): Unit = {
						val surfacePartition = crossProduct._1.map(_.asInstanceOf[String])
						val  wikiPages = crossProduct._2.map(_.asInstanceOf[WikiPage])
						println(s"ENTIRETEXTSURFACES: Crossing ${surfacePartition.size} surfaces with ${wikiPages.size} wiki pages.")
						var trie = new Trie()
						surfacePartition.foreach { surface => trie.add(surface)}

						wikiPages.foreach { wikiPage =>
							val tokens = TokenizerHelper.tokenize(wikiPage.plainText)

							val resultSurfaces = mutable.HashSet[String]()

							// each word and its following words must be checked, if it is a surface
							for (i <- 0 until tokens.size) {
								resultSurfaces ++= trie.slidingContains(tokens, i).map {
									containment => containment.mkString(" ")
								}
							}
							resultSurfaces.toIterator.map { surface => EntireTextSurfaces(wikiPage.pageTitle, surface)}.foreach { record =>
								out.collect(record)
							}
						}
						trie = null
					}
				}
			).name("Entire-Text-Surfaces-Along-With-Document")

		val surfaceDocumentCounts = env.readTextFile(surfaceDocumentCountsPath)

		val entireTextSurfaceCounts = entireTextSurfaces
			.groupBy { _.surface }
			.reduceGroup { group =>
				val surfaces = group.toList
				EntireTextSurfaceCounts(surfaces.head.surface, surfaces.size)
			}
			.name("Entire-Text-Surface-Counts")

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
