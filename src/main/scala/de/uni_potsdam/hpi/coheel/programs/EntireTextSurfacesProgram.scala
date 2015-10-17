package de.uni_potsdam.hpi.coheel.programs

import java.util.Date

import de.uni_potsdam.hpi.coheel.datastructures.NewTrie
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.programs.DataClasses.{EntireTextSurfaceCounts, EntireTextSurfaces, Plaintext}
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

class EntireTextSurfacesProgram extends CoheelProgram[Int] {

	val arguments = if (runsOffline()) List(-1) else 1 to 10
	override def getDescription = "Wikipedia Extraction: Entire Text Surfaces"

	override def buildProgram(env: ExecutionEnvironment, param: Int): Unit = {
		val plainTexts = readPlainTexts

		val currentFile = if (runsOffline()) "" else s"/$param"
		val surfaces = readSurfaces(currentFile)

		val entireTextSurfaces = plainTexts
			.flatMap(new FindEntireTextSurfacesFlatMap)
			.withBroadcastSet(surfaces, SurfacesInTrieFlatMap.BROADCAST_SURFACES)
			.name("Entire-Text-Surfaces-Along-With-Document")

		val surfaceDocumentCounts = readSurfaceDocumentCounts

		val entireTextSurfaceCounts = entireTextSurfaces
			.groupBy { _.surface }
			.reduceGroup { group =>
				val head = group.next().surface
				EntireTextSurfaceCounts(head, group.size + 1)
			}
			.name("Entire-Text-Surface-Counts")

		val surfaceLinkProbs = surfaceDocumentCounts.join(entireTextSurfaceCounts)
			.where { _.surface }
			.equalTo { _.surface }
			.map { joinResult => joinResult match {
				case (surfaceAsLinkCount, entireTextSurfaceCount) =>
					(surfaceAsLinkCount.surface, surfaceAsLinkCount.count, entireTextSurfaceCount.count,
						surfaceAsLinkCount.count.toDouble / entireTextSurfaceCount.count.toDouble)
			}
		}.name("Surface-Link-Probs")


		entireTextSurfaces.writeAsTsv(entireTextSurfacesPath + currentFile)
		surfaceLinkProbs.writeAsTsv(surfaceLinkProbsPath + currentFile)
	}
}

class FindEntireTextSurfacesFlatMap extends SurfacesInTrieFlatMap[Plaintext, EntireTextSurfaces] {
	var lastChunk = new Date()
	var i = 0
	val OUTPUT_EVERY = if (CoheelProgram.runsOffline()) 1000 else 10000
	override def flatMap(plainText: Plaintext, out: Collector[EntireTextSurfaces]): Unit = {
		if (i % OUTPUT_EVERY == 0) {
			val nextChunk = new Date()
			val difference = nextChunk.getTime - lastChunk.getTime
			log.info(f"EntireTextSurfaces update: Finished $i%6s, last $OUTPUT_EVERY took $difference%7s ms, remaining memory: ${FreeMemory.get()}%5s MB")
			lastChunk = nextChunk
		}
		i += 1
		findEntireTextSurfaces(plainText, trie).foreach(out.collect)
	}

	def findEntireTextSurfaces(plainText: Plaintext, trie: NewTrie): Iterator[EntireTextSurfaces] = {
		val text = plainText.plainText
		trie.findAllIn(text).map { surface => EntireTextSurfaces(plainText.pageTitle, surface)}
	}
}
