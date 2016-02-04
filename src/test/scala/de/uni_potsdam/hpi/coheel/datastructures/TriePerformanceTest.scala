package de.uni_potsdam.hpi.coheel.datastructures

import java.io.{File, StringReader}
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.util.PerformanceTimer
import de.uni_potsdam.hpi.coheel.wiki.{TokenizerHelper, WikiPageReader}
import org.scalatest.FunSuite

import scala.io.Source

object TriePerformanceTest {

	def main(args: Array[String]): Unit = {
//		val trie = new TrieToni
//		trie.add("angela")
//		println(trie.contains("angela"))
	}
}
class TriePerformanceTest extends FunSuite {

	test("performance of the trie") {
		testPerformance()
	}

	var set: Set[String] = null

	def testPerformance(): Unit = {
		PerformanceTimer.startTime("READING")
		print("Setup    :")
		val memoryBeforeSurfaces = FreeMemory.get(true, 10)
		val tokenizedSurfaces = readSurfaces(1000000)
		val memoryAfterSurfaces = FreeMemory.get(true, 10)
		val wikiText = readWikiText()
		val memoryAfterWiki = FreeMemory.get(true, 10)
		println(s" Done in ${PerformanceTimer.endTime("READING") / 1000} s.")

		println(s"Test Case: Load ${tokenizedSurfaces.size} surfaces into the trie. " +
			s"This uses ${memoryBeforeSurfaces - memoryAfterSurfaces} MB. " +
			s"Then find all occurrences in wiki page text. " +
			s"This uses ${memoryAfterSurfaces - memoryAfterWiki} MB.")
		println()

		println("=" * 80)
		val RUNS = 3
		List(
			("HashTrie with word-boundaries", () => new HashTrie())
//			, ("Toni's trie implementation", () => new TrieToni())
			, ("New trie", () => new NewTrie())
//			, ("HashTrie with char-boundaries", () => new HashTrie({ text => text.map(_.toString).toArray }))
//			, ("PatriciaTrie", () => new PatriciaTrieWrapper())
//			, ("ConcurrentTrie", () => new ConcurrentTreesTrie())
		).foreach { case (testName, trieCreator) =>
			for (i <- 1 to RUNS) {
				var trie = trieCreator.apply()
				PerformanceTimer.startTime(s"FULL-TRIE $testName $i")
				PerformanceTimer.startTime(s"TRIE-ADDING $testName $i")
				loadIntoTrie(tokenizedSurfaces, trie)
				val addTime = PerformanceTimer.endTime(s"TRIE-ADDING $testName $i")
				PerformanceTimer.startTime(s"TRIE-CHECKING $testName $i")
				val result = trie.findAllIn(wikiText)
				if (i == RUNS) {
					println(result.size)
//					if (set == null)
//						set = result.toSet
//					else
//						println(set -- result.toSet)
//					println(result.toList.sorted)
//					result.toList.sorted.foreach(println)
				}
				val checkTime = PerformanceTimer.endTime(s"TRIE-CHECKING $testName $i")
				val totalTime = PerformanceTimer.endTime(s"FULL-TRIE $testName $i")
				val memoryWithTrie = FreeMemory.get(true, 3)
				trie = null
				val memoryWithoutTrie = FreeMemory.get(true, 3)

				if (i == RUNS) {
					println(s"$testName")
					println(s"Time for adding   : $addTime ms")
					println(s"Time for checking : $checkTime ms")
					println(s"Total time        : $totalTime ms")
					println(s"Memory consumption: ${memoryWithoutTrie - memoryWithTrie} MB")
					println("=" * 80)
				}
			}
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
			val tokens = TokenizerHelper.tokenize(line, false).mkString(" ")
			if (tokens.isEmpty)
				None
			else
				Some(tokens)
		}.toArray
	}

	def readWikiText(nrPages: Int = 1000): String = {
		val classLoader = getClass.getClassLoader
		val reader = new WikiPageReader()
		val fileContent = "<root>" +
			Source.fromFile(classLoader.getResource("chunk/enwiki-latest-pages-articles1.xml-p000000010p000010000").getFile).getLines().mkString +
			"</root>"
		reader.xmlToWikiPages(new StringReader(fileContent)).take(nrPages).map(_.source).mkString(" ")
	}
}

