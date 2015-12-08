package de.uni_potsdam.hpi.coheel.programs

import java.util.Date

import de.uni_potsdam.hpi.coheel.datastructures.NewTrie
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.programs.DataClasses.{EntireTextSurfaceCounts, EntireTextSurfaces, PlainText}
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

class EntireTextSurfacesProgram extends CoheelProgram[String] {

	val arguments = if (runsOffline()) List("") else List("12345", "678910")
	override def getDescription = "Wikipedia Extraction: Entire Text Surfaces"

	override def buildProgram(env: ExecutionEnvironment, param: String): Unit = {
		val plainTexts = readPlainTexts

		val currentFile = if (runsOffline()) "" else s"/$param"
		val surfaces = readSurfaces(currentFile)

		val trieHits = plainTexts
			.flatMap(new FindEntireTextSurfacesFlatMap)
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
}

class FindEntireTextSurfacesFlatMap extends SurfacesInTrieFlatMap[PlainText, EntireTextSurfaces] {
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
		// TODO: Apparently, pygmalion is out.collect'ed here, even though it's not a full surface on node/file 8. Only run with argument 8 above and check
		trie.findAllIn(text).map { surface => EntireTextSurfaces(plainText.pageTitle, surface)}
	}
}
