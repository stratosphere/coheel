package de.uni_potsdam.hpi.coheel

import java.io.File

import de.uni_potsdam.hpi.coheel.datastructures.Trie
import de.uni_potsdam.hpi.coheel.programs.OutputFiles
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.io.Source

class TriePerformanceTest extends FunSuite {

	test("performance of the trie") {
		println("Started.")
		val classLoader = getClass.getClassLoader
		val file = new File(classLoader.getResource("trie_performance").getFile)
		println("Get file.")

		val lines = Source.fromFile(file).getLines().toList
		println("Read lines.")
		val tokenized = lines.flatMap { line =>
			val surface = line.split('\t')(0)
			val tokens = TokenizerHelper.tokenize(surface)
			if (tokens.isEmpty)
				None
			else
				Some(tokens)
		}
		println("Tokenized.")

		(1 to 20).foreach { i =>
			println(i)
			val trie = new Trie()
			if (i > 10) {
				PerformanceTimer.startTimeFirst(s"FULL-TRIE")
				PerformanceTimer.startTimeFirst(s"FULL-TRIE ${i - 10}")
			}
			tokenized.foreach { tokens =>
				trie.add(tokens)
			}
			tokenized.foreach { tokens =>
				val contains = trie.contains(tokens)
				assert(contains.asEntry)
			}
			if (i > 10)
				PerformanceTimer.endTimeFirst(s"FULL-TRIE ${i - 10}")
		}
		PerformanceTimer.endTimeFirst(s"FULL-TRIE")
		PerformanceTimer.printTimerEvents()
	}

}

