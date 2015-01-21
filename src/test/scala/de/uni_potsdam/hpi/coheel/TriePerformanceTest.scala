package de.uni_potsdam.hpi.coheel

import java.io.File

import de.uni_potsdam.hpi.coheel.datastructures._
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper
import org.scalatest.FunSuite
import scala.collection.mutable

import scala.io.Source

object TriePerformanceTest {

	type Histogram = mutable.Map[Int, Int]

	def main(args: Array[String]): Unit = {
		val testEnv = new TriePerformanceTest
		val trie = new HashTrie()
		print("Reading surfaces ..")
		val surfaces = testEnv.readSurfaces(1000)
		println(" Done.")
		print("Loading into trie ..")
		testEnv.loadIntoTrie(surfaces, trie)
		println(" Done.")

		val childrenQueue = mutable.Queue[(HashTrie, String, Int)]()

		val edgesInLevel = mutable.Map[Int, Int]().withDefaultValue(0)
		val levelHistogram = mutable.Map[Int, Histogram]()

		case class CommonPrefix(prefix: String, count: Int)
		implicit val ordering = Ordering.by[CommonPrefix, Int] { prefix => -prefix.count }
		val levelPrefixes = mutable.Map[Int, mutable.PriorityQueue[CommonPrefix]]()

		childrenQueue.enqueue((trie, "", 0))
		while (childrenQueue.nonEmpty) {
			val (current, strSoFar, level) = childrenQueue.dequeue()
			val children = current.children

			if (children != null) {
				if (!levelHistogram.contains(level))
					levelHistogram(level) = mutable.Map[Int, Int]().withDefaultValue(0)
				levelHistogram(level)(children.size) += 1

				if (!levelPrefixes.contains(level))
					levelPrefixes(level) = mutable.PriorityQueue()
				val prioQueue = levelPrefixes(level)
				prioQueue += CommonPrefix(strSoFar, children.size)
				if (prioQueue.size > 5)
					prioQueue.dequeue()

				edgesInLevel(level) += children.size

				children.foreach { case (str, next) =>
					childrenQueue.enqueue((next, strSoFar + " " + str, level + 1))
				}
			}
		}
		println(edgesInLevel.toList.sortBy(_._1))
		println("HISTOGRAMS")
		levelHistogram.toList.sortBy(_._1).foreach { case (level, histogram) =>
			println(s"Level: $level")
			histogram.toList.sortBy(_._1).foreach { case (size, count) =>
				println(f"$size%10d: $count%10d")
			}
			println("-" * 100)
		}
		println("=" * 100)
		println("LEVEL PREFIXES")
		levelPrefixes.toList.sortBy(_._1).foreach { case (level, prioQueue) =>
			println(level)
			println(prioQueue)
			println("-" * 100)
		}
	}
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
			loadIntoTrie(tokenizedSurfaces, trie)
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


	def loadIntoTrie(tokenizedSurfaces: Array[String], trie: Trie): Unit = {
		tokenizedSurfaces.foreach { tokens =>
			trie.add(tokens)
		}
	}

	def readSurfaces(nrLines: Int = Int.MaxValue): Array[String] = {
		val classLoader = getClass.getClassLoader
		val surfacesFile = new File(classLoader.getResource("surfaces").getFile)
		val lines = Source.fromFile(surfacesFile).getLines().take(nrLines)
		lines.flatMap { line =>
			val tokens = TokenizerHelper.tokenize(line).mkString(" ")
			if (tokens.isEmpty)
				None
			else
				Some(tokens)
		}.toArray
	}

	def readWikiText(nrLines: Int = 1000): String = {
		val classLoader = getClass.getClassLoader
		val wikiFile = new File(classLoader.getResource("chunk/enwiki-latest-pages-articles1.xml-p000000010p000010000").getFile)
		Source.fromFile(wikiFile).getLines().take(nrLines).mkString(" ")
	}
}

