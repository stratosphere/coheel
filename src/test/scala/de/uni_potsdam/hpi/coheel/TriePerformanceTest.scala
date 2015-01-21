package de.uni_potsdam.hpi.coheel

import java.io.File

import de.uni_potsdam.hpi.coheel.datastructures.{AhoCorasickTrie, ConcurrentTreesTrie, PatriciaTrieWrapper, HashTrie}
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper
import org.scalatest.events.Event
import org.scalatest.{Reporter, Args, FunSuite}

import scala.io.Source

object TriePerformanceTest extends Reporter {

	def main(args: Array[String]): Unit = {
		val testEnv = new TriePerformanceTest
		testEnv.testPerformance()
	}
	override def apply(event: Event): Unit = {  }
}
class TriePerformanceTest extends FunSuite {

	test("performance of the trie") {
		testPerformance()
	}

	def testPerformance(): Unit = {
		print("Setup    :")
		val memoryBeforeSurfaces = FreeMemory.get(true, 10)
		val tokenizedSurfaces = readSurfaces()
		val memoryAfterSurfaces = FreeMemory.get(true, 10)
		val wikiText = readWikiText()
		val memoryAfterWiki = FreeMemory.get(true, 10)
		println(" Done.")

		println(s"Test Case: Load ${tokenizedSurfaces.size} surfaces into the trie. " +
			s"This uses ${memoryBeforeSurfaces - memoryAfterSurfaces} MB. " +
			s"Then find all occurrences in wiki page text. " +
			s"This uses ${memoryAfterSurfaces - memoryAfterWiki} MB.")
		println()

		println("=" * 80)
		List(
			("HashTrie with word-boundaries", () => new HashTrie())
//			, ("HashTrie with char-boundaries", () => new HashTrie({ text => text.map(_.toString).toArray }))
			, ("PatriciaTrie", () => new PatriciaTrieWrapper())
//			, ("ConcurrentTrie", () => new ConcurrentTreesTrie())
		).foreach { case (testName, trieCreator) =>
			var trie = trieCreator.apply()
			PerformanceTimer.startTime(s"FULL-TRIE $testName")
			PerformanceTimer.startTime(s"TRIE-ADDING $testName")
			tokenizedSurfaces.foreach { tokens =>
				trie.add(tokens)
			}
			val addTime = PerformanceTimer.endTime(s"TRIE-ADDING $testName")
			PerformanceTimer.startTime(s"TRIE-CHECKING $testName")
			println(trie.findAllIn(wikiText).size)
			val checkTime = PerformanceTimer.endTime(s"TRIE-CHECKING $testName")
			val totalTime = PerformanceTimer.endTime(s"FULL-TRIE $testName")
			val memoryWithTrie = FreeMemory.get(true, 10)
			trie = null
			val memoryWithoutTrie = FreeMemory.get(true, 10)

			println(s"$testName")
			println(s"Time for adding   : $addTime ms")
			println(s"Time for checking : $checkTime ms")
			println(s"Total time        : $totalTime ms")
			println(s"Memory consumption: ${memoryWithoutTrie - memoryWithTrie} MB")
			println("=" * 80)
		}
	}


	def readSurfaces(): Array[String] = {
		val classLoader = getClass.getClassLoader
		val surfacesFile = new File(classLoader.getResource("surfaces").getFile)
		val lines = Source.fromFile(surfacesFile).getLines().take(1000000)
		lines.flatMap { line =>
			val tokens = TokenizerHelper.tokenize(line).mkString(" ")
			if (tokens.isEmpty)
				None
			else
				Some(tokens)
		}.toArray
	}

	def readWikiText(): String = {
		val classLoader = getClass.getClassLoader
		val wikiFile = new File(classLoader.getResource("chunk/enwiki-latest-pages-articles1.xml-p000000010p000010000").getFile)
		Source.fromFile(wikiFile).getLines().take(1000).mkString(" ")
	}
}

