package de.uni_potsdam.hpi.coheel

import java.io.File

import de.uni_potsdam.hpi.coheel.datastructures.Trie
import de.uni_potsdam.hpi.coheel.wiki.TextAnalyzer

import scala.io.Source

object TrieTester {

	def main(args: Array[String]): Unit = {
		printMemoryStatus()
		Thread.sleep(5000)
		buildTrie()
		printMemoryStatus()
	}

	def buildTrie(): Unit = {
		val lines = Source.fromFile(new File("testoutput/surfaces.wiki")).getLines()

		var i = 0
		val trie = new Trie()
		lines.foreach { line =>
			val tokens = TextAnalyzer.tokenize(line)
			if (tokens.nonEmpty)
				trie.add(tokens)
			i += 1
			if (i % 1000000 == 0) {
				println(f"$i")
				printMemoryStatus()
			}
		}
	}

	def printMemoryStatus(): Unit = {
		val maxMem   = Runtime.getRuntime.maxMemory().toDouble / 1024 / 1024
		val freeMem  = Runtime.getRuntime.freeMemory().toDouble / 1024 / 1024
		val totalMem = Runtime.getRuntime.totalMemory().toDouble / 1024 / 1024
		val actualMem = maxMem - (totalMem - freeMem)
//		println(f"Max  : $maxMem%.2f MB")
//		println(f"Free : $freeMem%.2f MB")
//		println(f"Total: $totalMem%.2f MB")
		println(f"Act. : $actualMem%.2f MB")
	}

}
