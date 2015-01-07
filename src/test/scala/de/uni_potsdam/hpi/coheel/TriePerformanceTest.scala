package de.uni_potsdam.hpi.coheel

import java.io.File

import de.uni_potsdam.hpi.coheel.datastructures.{ConcurrentTreesWrapper, PatriciaTrieWrapper, HashTrie}
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper
import org.scalatest.events.Event
import org.scalatest.{Reporter, Args, FunSuite}

import scala.io.Source

object TriePerformanceTest extends Reporter {
	val TEST_NAME = "performance of the trie"

	def main(args: Array[String]): Unit = {
		val testEnv = new TriePerformanceTest
		testEnv.runTest(TEST_NAME, Args(this))
	}
	override def apply(event: Event): Unit = {  }
}
class TriePerformanceTest extends FunSuite {
	import TriePerformanceTest._

	test(TEST_NAME) {
		print("Setup    :")
		val classLoader = getClass.getClassLoader
		val surfacesFile = new File(classLoader.getResource("surfaces").getFile)
		val lines = Source.fromFile(surfacesFile).getLines()
		val tokenized = lines.flatMap { line =>
			val tokens = TokenizerHelper.tokenize(line)
			if (tokens.isEmpty)
				None
			else
				Some(tokens)
		}
		println(" Done.")
		println(s"Test Case: Load ${tokenized.size} surfaces into the trie and then check each token for existence.")
		println()

		println("=" * 80)
		List(classOf[HashTrie], classOf[PatriciaTrieWrapper], classOf[ConcurrentTreesWrapper]).foreach { trieClass =>
			val testName = trieClass.getSimpleName
			PerformanceTimer.startTime(s"FULL-TRIE $testName")
			PerformanceTimer.startTime(s"TRIE-ADDING $testName")
			var trie = trieClass.newInstance()
			tokenized.foreach { tokens =>
				trie.add(tokens)
			}
			val addTime = PerformanceTimer.endTime(s"TRIE-ADDING $testName")
			PerformanceTimer.startTime(s"TRIE-CHECKING $testName")
			tokenized.foreach { tokens =>
				val contains = trie.contains(tokens)
				assert(contains.asEntry)
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
			println(s"Memory consumption: ${memoryWithTrie - memoryWithoutTrie} MB")
			println("=" * 80)
		}
	}
}

