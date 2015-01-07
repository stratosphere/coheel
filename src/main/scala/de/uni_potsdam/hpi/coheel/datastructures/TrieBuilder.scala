package de.uni_potsdam.hpi.coheel.datastructures

import java.io.File

import de.uni_potsdam.hpi.coheel.PerformanceTimer
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.io.OutputFiles
import OutputFiles._
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper
import org.apache.commons.collections4.trie.PatriciaTrie
import org.apache.flink.types.Nothing
import org.apache.log4j.Logger

import scala.io.Source

object TrieTester {
	def main(args: Array[String]): Unit = {
		TrieBuilder.printMemoryStatus()
		val trie = TrieBuilder.buildFullTrie()
		TrieBuilder.printMemoryStatus()
	}
}

/**
 * Singleton objects for the tries.
 */
object TrieBuilder {

	val log = Logger.getLogger(getClass)

	// this contains all surfaces
	var fullTrie: TrieLike = _

	// this contains only surfaces, above a certain percentage
	var thresholdTrie: TrieLike = _


	/**
	 * Private helper function to encapsulate reoccurring code for building a trie from an output file
	 * @param filePath The output file
	 * @param trieBuildingPart The method, which, given the line, parses the line and adds it to the trie.
	 */
	private def trieBuilderHelper(filePath: String, textMessage: String)(trieBuildingPart: String => Unit): Unit = {
		val fileName = filePath.replace("file://", "")
		val lines = Source.fromFile(new File(fileName)).getLines()

		var i = 0
		lines.foreach { line =>
			try {
				trieBuildingPart(line)
				i += 1
			} catch {
				case e @ (_: ArrayIndexOutOfBoundsException | _: NumberFormatException) =>
					log.warn(s"Bad line in input: ${e.toString}")
				case e: OutOfMemoryError =>
					log.error(e.toString)
					log.error(i.toString)
					System.exit(1)
			}
		}
		log.info(textMessage)
	}

	def buildThresholdTrie(threshold: Double): Unit = {
		PerformanceTimer.startTime(s"THRESHOLD-TRIE $threshold")
		if (thresholdTrie != null) {
			thresholdTrie = null
			// clean up trie
			for (i <- 1 to 5) System.gc()
		}
		thresholdTrie = new HashTrie()

		trieBuilderHelper(surfaceLinkProbsPath, s"Built threshold trie with threshold $threshold.") { line =>
			val split = line.split('\t')
			val surface = split(0)
			val prob = split(2).toDouble
			if (prob >= threshold) {
				val tokens = surface.split(' ')
				if (tokens.nonEmpty)
					thresholdTrie.add(tokens)
			}
		}
		PerformanceTimer.endTime(s"THRESHOLD-TRIE $threshold")
	}

	def buildFullTrie(trie: TrieLike = null): Unit = {
		PerformanceTimer.startTimeFirst(s"FULL-TRIE")
//		fullTrie = new PatriciaTrieWrapper()
//		fullTrie = new Trie()
		fullTrie = if (trie != null) trie else new ConcurrentTreesWrapper()

		val t1 = System.currentTimeMillis()
		trieBuilderHelper("../src/test/resources/surfaces", "Built full trie.") { line =>
//			val surface = line.split('\t')(0)
			val tokens = line.split(' ')//TokenizerHelper.tokenize(surface)
			if (tokens.nonEmpty) {
				try {
					fullTrie.add(tokens)
				}
				catch {
					case e: Throwable =>
						println(tokens)
						println(line)
				}
			}
		}
		val t2 = System.currentTimeMillis()
		println(t2 - t1)
		PerformanceTimer.endTimeFirst(s"FULL-TRIE")
	}

	/**
	 * Helper function for printing the memory status.
	 */
	def printMemoryStatus(): Unit = {
		val actualMem = FreeMemory.get()
		log.info(s"Free Memory: $actualMem MB")
	}

}
