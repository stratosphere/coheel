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
		val classLoader = getClass.getClassLoader
		val surfacesFile = new File(classLoader.getResource("surfaces").getFile)
		val lines = Source.fromFile(surfacesFile).getLines()
		val memoryBeforeLines = FreeMemory.get(true, 10)
		val tokenizedSurfaces = lines.flatMap { line =>
			val tokens = TokenizerHelper.tokenize(line).mkString(" ")
			if (tokens.isEmpty)
				None
			else
				Some(tokens)
		}.toArray
		val memoryAfterLines = FreeMemory.get(true, 10)
		println(" Done.")
		println(s"Test Case: Load ${tokenizedSurfaces.size} surfaces into the trie and then check each token for existence. " +
			s"This uses ${memoryBeforeLines - memoryAfterLines} MB.")
		println()

		println("=" * 80)
		List(
//			, classOf[AhoCorasickTrie]
			classOf[HashTrie]
			, classOf[PatriciaTrieWrapper]
			, classOf[ConcurrentTreesTrie]
		).foreach { trieClass =>
			val testName = trieClass.getSimpleName
			PerformanceTimer.startTime(s"FULL-TRIE $testName")
			PerformanceTimer.startTime(s"TRIE-ADDING $testName")
			var trie = trieClass.newInstance()
			tokenizedSurfaces.foreach { tokens =>
				trie.add(tokens)
			}
			val addTime = PerformanceTimer.endTime(s"TRIE-ADDING $testName")
			PerformanceTimer.startTime(s"TRIE-CHECKING $testName")
			tokenizedSurfaces.foreach { surfaceTokens =>
				val contains = trie.contains(surfaceTokens)
				if (!contains.asEntry) {
					println(surfaceTokens)
					throw new Exception(s"Token $surfaceTokens not contained.")
				}
			}
			val checkTime = PerformanceTimer.endTime(s"TRIE-CHECKING $testName")
			val totalTime = PerformanceTimer.endTime(s"FULL-TRIE $testName")
			val memoryWithTrie = FreeMemory.get(true, 10)
			trie = null
			val memoryWithoutTrie = FreeMemory.get(true, 10)

			println(s"${trieClass.getSimpleName}")
			println(s"Time for adding   : $addTime ms")
			println(s"Time for checking : $checkTime ms")
			println(s"Total time        : $totalTime ms")
			println(s"Memory consumption: ${memoryWithoutTrie - memoryWithTrie} MB")
			println("=" * 80)
		}
	}
}

