package de.uni_potsdam.hpi.coheel.programs

import java.util.Date

import de.uni_potsdam.hpi.coheel.datastructures.NewTrie
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.programs.DataClasses.{Plaintext, EntireTextSurfaces, EntireTextSurfaceCounts}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.apache.log4j.Logger

object EntireTextSurfacesProgram {
	val BROADCAST_SURFACES = "surfaces"
}
class EntireTextSurfacesProgram extends CoheelProgram[Int] {

	val params = if (runsOffline()) List(-1) else 1 to 10
	override def getDescription = "Wikipedia Extraction: Entire Text Surfaces"

	override def buildProgram(env: ExecutionEnvironment, param: Int): Unit = {
		val plainTexts = getPlainTexts

		val currentFile = if (runsOffline()) "" else s"/$param"
		val surfaces = getSurfaces(currentFile)
		val scores   = getScores()

		val entireTextSurfaces = plainTexts
			.flatMap(new FindEntireTextSurfacesFlatMap)
			.withBroadcastSet(surfaces, EntireTextSurfacesProgram.BROADCAST_SURFACES)
			.name("Entire-Text-Surfaces-Along-With-Document")

		val surfaceDocumentCounts = getSurfaceDocumentCounts

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

		val newScores = surfaceLinkProbs.join(scores)
			.where(0)
			.equalTo(0)
			.map { joinResult =>
				joinResult match {
					case ((_, _, _, surfaceLinkProb), (_, values)) =>
						values.toList.take(4).mkString("\t") + s"\t$surfaceLinkProb\t" + values.drop(4).mkString("\t")
				}

			}

		entireTextSurfaces.writeAsTsv(entireTextSurfacesPath + currentFile)
		surfaceLinkProbs.writeAsTsv(surfaceLinkProbsPath + currentFile)
		newScores.writeAsText(newScoresPath + currentFile)
	}
}
class FindEntireTextSurfacesFlatMap extends RichFlatMapFunction[Plaintext, EntireTextSurfaces] {
	var trie: NewTrie = _
	var lastChunk = new Date()
	def log = Logger.getLogger(getClass)

	override def open(params: Configuration): Unit = {
		log.info(s"Building trie with ${FreeMemory.get(true)} MB")
		val d1 = new Date
		trie = getRuntimeContext.getBroadcastVariableWithInitializer(EntireTextSurfacesProgram.BROADCAST_SURFACES, new TrieBroadcastInitializer)
		log.info(s"Finished trie initialization in ${(new Date().getTime - d1.getTime) / 1000} s")
		log.info(s"${FreeMemory.get(true)} MB of memory remaining")
	}
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
