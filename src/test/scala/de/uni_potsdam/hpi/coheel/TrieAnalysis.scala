package de.uni_potsdam.hpi.coheel

import scala.collection.mutable
import de.uni_potsdam.hpi.coheel.datastructures._

object TrieAnalysis {

	type Histogram = mutable.Map[Int, Int]
	def main(args: Array[String]): Unit = {
		val testEnv = new TriePerformanceTest
		val trie = new HashTrie()
		print("Reading surfaces ..")
		val surfaces = testEnv.readSurfaces(10000)
		println(s" ${surfaces.size} .. Done.")
		print("Loading into trie ..")
		testEnv.loadIntoTrie(surfaces, trie)
		println(" Done.")

		val childrenQueue = mutable.Queue[(HashTrie, String, Int)]()

		val edgesInLevel = mutable.Map[Int, Int]().withDefaultValue(0)
		val levelHistogram = mutable.Map[Int, Histogram]()

		case class CommonPrefix(prefix: String, count: Int)
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
					levelPrefixes(level) = mutable.PriorityQueue()(Ordering.by[CommonPrefix, Int] { prefix => -prefix.count })
				val prioQueue = levelPrefixes(level)
				prioQueue += CommonPrefix(strSoFar, children.size)
				if (prioQueue.size > 20)
					prioQueue.dequeue()

				edgesInLevel(level) += children.size

				val stringPrefix = if (strSoFar.isEmpty) "" else strSoFar + " "

				children.foreach { case (str, next) =>
					childrenQueue.enqueue((next, stringPrefix + str, level + 1))
				}
			}
		}
		println("EDGES IN LEVEL")
		println(edgesInLevel.toList.sortBy(_._1))
		println()
		println("HISTOGRAMS OF CHILDREN SIZES")
		levelHistogram.toList.sortBy(_._1).foreach { case (level, histogram) =>
			println(s"Level: $level")
			histogram.toList.sortBy(_._1).foreach { case (size, count) =>
				println(f"$size%10d: $count%10d")
			}
			println("-" * 100)
		}
		println("=" * 100)
		println()
		println("LEVEL PREFIXES")
		levelPrefixes.toList.sortBy(_._1).foreach { case (level, prioQueue) =>
			println(level)
			prioQueue.foreach { case CommonPrefix(s, i) =>
				println(f"$s%50s: $i")
			}
			println("-" * 100)
		}
	}
}
