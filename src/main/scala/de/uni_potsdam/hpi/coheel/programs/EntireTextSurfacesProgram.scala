package de.uni_potsdam.hpi.coheel.programs

import java.util.Date

import de.uni_potsdam.hpi.coheel.datastructures.{ConcurrentTreesWrapper, TrieLike, HashTrie}
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.programs.DataClasses.{EntireTextSurfaces, SurfaceAsLinkCount, EntireTextSurfaceCounts}
import de.uni_potsdam.hpi.coheel.wiki.{WikiPage, TokenizerHelper}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.apache.log4j.Logger
import scala.collection.JavaConverters._

import scala.collection.mutable

object EntireTextSurfacesProgram {
	val BROADCAST_SURFACES = "surfaces"
}
class EntireTextSurfacesProgram extends CoheelProgram {

	@transient val log = Logger.getLogger(getClass)
	// prepare the trie
//	TrieBuilder.buildFullTrie()

	override def getDescription = "Wikipedia Extraction: Entire Text Surfaces"

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val wikiPages = ProgramHelper.filterNormalPages(ProgramHelper.getWikiPages(env))
		val surfaces = env.readTextFile(surfaceProbsPath).flatMap { line =>
			val split = line.split('\t')
			if (split.size == 3)
				Some(split(0))
			else
				None
		}.name("Surfaces")

		val entireTextSurfaces = wikiPages
			.flatMap(new FindEntireTextSurfacesFlatMap)
			.withBroadcastSet(surfaces, EntireTextSurfacesProgram.BROADCAST_SURFACES)
			.name("Entire-Text-Surfaces-Along-With-Document")

		val surfaceDocumentCounts = env.readTextFile(surfaceDocumentCountsPath).name("Raw-Surface-Document-Counts")

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
		}.name("Surface-Document-Counts").join(entireTextSurfaceCounts)
			.where { _.surface }
			.equalTo { _.surface }
			.map { joinResult => joinResult match {
				case (surfaceAsLinkCount, entireTextSurfaceCount) =>
					(surfaceAsLinkCount.surface, entireTextSurfaceCount.count,
						surfaceAsLinkCount.count.toDouble / entireTextSurfaceCount.count.toDouble)
			}
		}.name("Surface-Link-Probs")

		entireTextSurfaces.writeAsTsv(entireTextSurfacesPath)
		surfaceLinkProbs.writeAsTsv(surfaceLinkProbsPath)
	}
}
class FindEntireTextSurfacesFlatMap extends RichFlatMapFunction[WikiPage, EntireTextSurfaces] {
	var trie: TrieLike = _
	var last1000 = new Date()

	var i = 0
	override def open(params: Configuration): Unit = {
//		trie = new HashTrie
		trie = new ConcurrentTreesWrapper
		println(s"Free memory, before: ${FreeMemory.get(true)} MB")
		getRuntimeContext.getBroadcastVariable[String](EntireTextSurfacesProgram.BROADCAST_SURFACES).asScala.foreach { surface =>
			trie.add(surface)
		}
		println(s"Free memory, after: ${FreeMemory.get(true)} MB")
	}
	override def flatMap(wikiPage: WikiPage, out: Collector[EntireTextSurfaces]): Unit = {
		if (i % 1000 == 0) {
			val new1000 = new Date()
			val difference = new1000.getTime - last1000.getTime
			println(s"${new Date()}: ENTIRETEXTSURFACES: $i, LAST 1000: $difference ms, FREE MEMORY: ${FreeMemory.get(true)} MB")
		}
		i += 1
		findEntireTextSurfaces(wikiPage, trie).foreach(out.collect)
	}

	def findEntireTextSurfaces(wikiPage: WikiPage, trie: TrieLike): Iterator[EntireTextSurfaces] = {
		val tokens = TokenizerHelper.transformToTokenized(wikiPage.plainText, false)

		val entireTextSurfaces = trie.asInstanceOf[ConcurrentTreesWrapper].getKeysContainedIn(tokens).toSet

		entireTextSurfaces.map { surface =>
			EntireTextSurfaces(wikiPage.pageTitle, surface.toString)
		}.toIterator
//
//		val tokens = TokenizerHelper.tokenize(wikiPage.plainText, false)
//		val resultSurfaces = mutable.HashSet[String]()
//
//		// each word and its following words must be checked, if it is a surface
//		for (i <- 0 until tokens.size) {
//			resultSurfaces ++= trie.slidingContains(tokens, i).map {
//				containment => containment.mkString(" ")
//			}
//		}
//		resultSurfaces.toIterator.map { surface => EntireTextSurfaces(wikiPage.pageTitle, surface)}
	}
}
