package de.uni_potsdam.hpi.coheel.programs

import java.util.Date

import de.uni_potsdam.hpi.coheel.datastructures.NewTrie
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.programs.DataClasses.{EntireTextSurfaceCounts, EntireTextSurfaces, PlainText}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
  * Calculates the surface link probs.
  *
  * This needs the surface documents file in two halfs under the
  * `surfaceDocumentCountsHalfsPath` path. This file can be created
  * with bin/prepare-surface-link-probs-program.sh.
  */
class SurfaceLinkProbsProgram extends CoheelProgram[String] {

	import CoheelLogger._


	val arguments = if (runsOffline()) List("1") else List("12345", "678910")
	override def getDescription = "Wikipedia Extraction: Entire Text Surfaces"

	override def buildProgram(env: ExecutionEnvironment, param: String): Unit = {
		val plainTexts = readPlainTexts

		val currentFile = if (runsOffline()) "" else s"/$param"
		val surfaces = readSurfaces(currentFile)

		val trieHits = plainTexts
			.flatMap(new RunTrieOverPlainTextFlatMap)
			.withBroadcastSet(surfaces, SurfacesInTrieFlatMap.BROADCAST_SURFACES)
			.name("Entire-Text-Surfaces-Along-With-Document")

		val surfaceDocumentCounts = readSurfaceDocumentCounts()

		val entireTextSurfaceCounts = trieHits
			.groupBy { _.surface }
			.reduceGroup { group =>
				val surface = group.next().surface
				EntireTextSurfaceCounts(surface, group.size + 1) // + 1 because we already consumed one element from the iterator
			}
			.name("Entire-Text-Surface-Counts")

		val surfaceLinkProbs = surfaceDocumentCounts.join(entireTextSurfaceCounts)
			.where { _.surface }
			.equalTo { _.surface }
			.map { joinResult => joinResult match {
				case (surfaceAsLinkCount, entireTextSurfaceCount) =>
					(surfaceAsLinkCount.surface, surfaceAsLinkCount.count, entireTextSurfaceCount.count,
						surfaceAsLinkCount.count.toDouble / entireTextSurfaceCount.count.toDouble, param)
			}
		}.name("Surface-Link-Probs")

		trieHits.writeAsTsv(trieHitPath + currentFile)
		entireTextSurfaceCounts.writeAsTsv(entireTextSurfacesPath + currentFile)
		surfaceLinkProbs.writeAsTsv(surfaceLinkProbsPath + currentFile)
	}

	def readSurfaces(subFile: String = ""): DataSet[String] = {
		environment.readTextFile(surfaceDocumentCountsHalfsPath + subFile).name("Subset of Surfaces")
			.flatMap(new RichFlatMapFunction[String, String] {
				override def flatMap(line: String, out: Collector[String]): Unit = {
					val split = line.split('\t')
					if (split.length == 3)
						out.collect(split(0))
					else {
						log.warn(s"SurfaceProbs: Discarding '${split.deep}' because split size not correct")
						log.warn(line)
					}
				}
			}).name("Parsed Surfaces")
	}
}

class RunTrieOverPlainTextFlatMap extends SurfacesInTrieFlatMap[PlainText, EntireTextSurfaces] {
	import CoheelLogger._
	var lastChunk = new Date()
	var i = 0
	val OUTPUT_EVERY = if (CoheelProgram.runsOffline()) 1000 else 10000
	override def flatMap(plainText: PlainText, out: Collector[EntireTextSurfaces]): Unit = {
		if (i % OUTPUT_EVERY == 0) {
			val nextChunk = new Date()
			val difference = nextChunk.getTime - lastChunk.getTime
			log.info(f"EntireTextSurfaces update: Finished $i%6s, last $OUTPUT_EVERY took $difference%7s ms, remaining memory: ${FreeMemory.get()}%5s MB")
			lastChunk = nextChunk
		}
		i += 1
		findEntireTextSurfaces(plainText, trie).foreach(out.collect)
	}

	def findEntireTextSurfaces(plainText: PlainText, trie: NewTrie): Iterator[EntireTextSurfaces] = {
		val text = plainText.plainText
		trie.findAllIn(text).map { surface => EntireTextSurfaces(plainText.pageTitle, surface)}
	}
}
