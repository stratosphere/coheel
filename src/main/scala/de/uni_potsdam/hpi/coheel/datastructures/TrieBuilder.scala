package de.uni_potsdam.hpi.coheel.datastructures

import java.io.File

import de.uni_potsdam.hpi.coheel.programs.OutputFiles._
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper
import org.slf4s.Logging

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
object TrieBuilder extends Logging {

	// this contains all surfaces
	var fullTrie: Trie = _

	// this contains only surfaces, above a certain percentage
	var thresholdTrie: Trie = _


	/**
	 * Private helper function to encapsulate reoccurring code for building a trie from an output file
	 * @param filePath The output file
	 * @param trieBuildingPart The method, which, given the line, parses the line and adds it to the trie.
	 */
	private def trieBuilderHelper(filePath: String)(trieBuildingPart: String => Unit): Unit = {
		val fileName = filePath.replace("file://", "")
		val lines = Source.fromFile(new File(fileName)).getLines()

		var i = 0
		lines.foreach { line =>
			try {
				trieBuildingPart(line)
				val surface = line.split('\t')(0)
				val tokens = TokenizerHelper.tokenize(surface)
				if (tokens.nonEmpty)
					fullTrie.add(tokens)
				i += 1
//				if (i % 1000000 == 0) {
//					log.info(f"$i")
//					printMemoryStatus()
//				}
			} catch {
				case e: OutOfMemoryError =>
					log.error(e.toString)
					log.error(i.toString)
					System.exit(1)
			}
		}
		log.info("Built trie.")
	}
	def buildThresholdTrie(threshold: Double): Unit = {
		thresholdTrie = new Trie()

//		trieBuilderHelper() { line =>
//			val surface = line.split('\t')(0)
//			val tokens = TokenizerHelper.tokenize(surface)
//			if (tokens.nonEmpty)
//				fullTrie.add(tokens)
//		}
	}

	def buildFullTrie(): Unit = {
		fullTrie = new Trie()

		trieBuilderHelper(surfaceProbsPath) { line =>
			val surface = line.split('\t')(0)
			val tokens = TokenizerHelper.tokenize(surface)
			if (tokens.nonEmpty)
				fullTrie.add(tokens)
		}
	}

	/**
	 * Helper function for printing the memory status.
	 */
	def printMemoryStatus(): Unit = {
		val maxMem   = Runtime.getRuntime.maxMemory().toDouble / 1024 / 1024
		val freeMem  = Runtime.getRuntime.freeMemory().toDouble / 1024 / 1024
		val totalMem = Runtime.getRuntime.totalMemory().toDouble / 1024 / 1024
		val actualMem = maxMem - (totalMem - freeMem)
		log.info(f"Act. : $actualMem%.2f MB")
	}

}
