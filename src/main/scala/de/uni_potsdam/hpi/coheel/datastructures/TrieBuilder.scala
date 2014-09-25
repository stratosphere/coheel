package de.uni_potsdam.hpi.coheel.datastructures

import java.io.{BufferedWriter, File, FileWriter}

import de.uni_potsdam.hpi.coheel.FlinkProgramRunner
import de.uni_potsdam.hpi.coheel.programs.OutputFiles._
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper
import org.slf4s.Logging

import scala.io.Source

object TrieTester {

	def main(args: Array[String]): Unit = {
		val trieBuilder = new TrieBuilder
//		trieBuilder.printMemoryStatus()
		trieBuilder.tokenizeSurfaces()
//		val trie = trieBuilder.buildTrie()
		trieBuilder.printMemoryStatus()
	}

}
object TrieBuilder {

	// builds the trie by accessing the lazy value
	def build(): Unit = trie

	lazy val trie: Trie = {
		new TrieBuilder().buildTrie()
	}
}
class TrieBuilder extends Logging {

	def printMemoryStatus(): Unit = {
		val maxMem   = Runtime.getRuntime.maxMemory().toDouble / 1024 / 1024
		val freeMem  = Runtime.getRuntime.freeMemory().toDouble / 1024 / 1024
		val totalMem = Runtime.getRuntime.totalMemory().toDouble / 1024 / 1024
		val actualMem = maxMem - (totalMem - freeMem)
		log.info(f"Act. : $actualMem%.2f MB")
	}

	def tokenizeSurfaces(): Unit = {
		val fileName = surfaceProbsPath.replace("file://", "")
		val lines = Source.fromFile(new File(fileName)).getLines()

		var i = 0
		lines.foreach { line =>
			val surface = line.split('\t')(0)
			val tokens = TokenizerHelper.tokenize(surface)
			if (tokens.nonEmpty) {
				println(tokens.mkString("\t"))
			}
			i += 1
			if (i % 1000000 == 0) {
				log.info(f"$i")
				printMemoryStatus()
			}
		}
	}
	def buildTrie(): Trie = {
		val fileName = surfaceProbsPath.replace("file://", "")
		val lines = Source.fromFile(new File(fileName)).getLines()

		var i = 0
		val trie = new Trie()
		lines.foreach { line =>
			try {
				val surface = line.split('\t')(0)
				val tokens = TokenizerHelper.tokenize(surface)
				if (tokens.nonEmpty)
					trie.add(tokens)
				i += 1
				if (i % 1000000 == 0) {
//					log.info(f"$i")
//					printMemoryStatus()
				}
			} catch {
				case e: OutOfMemoryError =>
					log.error(e.toString)
					log.error(i.toString)
					System.exit(1)
			}
		}
		log.info("Built trie.")
		trie
	}

}
