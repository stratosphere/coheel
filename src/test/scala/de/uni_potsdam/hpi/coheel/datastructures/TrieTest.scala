package de.uni_potsdam.hpi.coheel.datastructures

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class TrieTest extends FunSuite {

	def newTrie(): TrieLike = {
		val trie = new HashTrie
		trie
	}

	test("single word queries work") {
		val trie = newTrie()
		trie.add("angela")
		assert(trie.contains("angela").asEntry)
	}

	test("multiple word queries work") {
		val trie = newTrie()
		trie.add("angela merkel")
		assert(trie.contains("angela merkel").asEntry)
	}

	test("distinction between contains-asEntry and contains-asIntermediateNode") {
		val trie = newTrie()
		trie.add("angela dorothea merkel")

		assert(trie.contains("angela").asIntermediateNode)
		assert(trie.contains("angela dorothea").asIntermediateNode)
		assert(trie.contains("angela dorothea merkel").asIntermediateNode)

		assert(!trie.contains("angela").asEntry)
		assert(!trie.contains("angela dorothea").asEntry)
		assert(trie.contains("angela dorothea merkel").asEntry)
	}

	test("only actually added words are considered") {
		val trie = newTrie()
		trie.add("angela merkel")
		assert(!trie.contains("angela").asEntry)
	}

	test("multiple adds do not cause harm") {
		val trie = newTrie()
		trie.add("angela merkel")
		trie.add("angela merkel")
		trie.add("angela merkel")
		assert(!trie.contains("angela").asEntry)
		assert(trie.contains("angela merkel").asEntry)
	}

	test("sliding contains works as expected") {
		val trie = newTrie()
		trie.add("angela merkel")
		trie.add("angela merkel is german")
		trie.add("angela")
		trie.add("merkel")

		val testSentence = Array("angela", "merkel", "is", "german")
		val result1 = trie.slidingContains(testSentence, 0).map { containment => containment.mkString(" ")}
		val expected1 = Seq("angela", "angela merkel", "angela merkel is german")
		println(result1)
		expected1.foreach { expected =>
			assert(result1.contains(expected))
		}

		val result2 = trie.slidingContains(testSentence, 1).map { containment => containment.mkString(" ") }
//		println(result2)
		assert(result2.size === 1)
		assert(result2.contains("merkel"))
	}

	ignore("get keysContainedIn for concurrent trees implementation") {
		val trie = new ConcurrentTreesTrie
		trie.add("another one bites the dust")
		trie.add("angela merkel")
		trie.add("angela merkel is german")
		trie.add("angela")
		trie.add("merkel")

		val expected1 = Seq("angela", "angela merkel", "angela merkel is german", "merkel")
		val result1 = trie.rt.getKeysContainedIn("angela merkel is german").asScala.toList
		println(result1)
		expected1.foreach { expected =>
			assert(result1.contains(expected))
		}
	}

	test("sliding contains works as expected for arbitrary type") {
		case class Foo(a: Int, b: Double)

		val trie = newTrie()
		trie.add("1 2")
		trie.add("1 2 3")

		val testSentence = Array(Foo(1, 1.0), Foo(2, 2.0))
		val result = trie.slidingContains[Foo](testSentence, { f => f.a.toString }, 0)
		assert (result.size === 1)
		assert (result(0)(0).a === 1 && result(0)(1).a === 2)

	}

	test("branching works at every level") {
		val trie = newTrie()
		trie.add("angela dorothea merkel")
		trie.add("angela dorothea kanzler")
		assert(!trie.contains("dorothea").asEntry)
		assert(!trie.contains("angela dorothea").asEntry)
		assert(trie.contains("angela dorothea merkel").asEntry)
	}

	test("can go many levels deep") {
		val trie = newTrie()
		trie.add("ab cd ef gh ij kl")
		assert(trie.contains("ab cd ef gh ij kl").asEntry)
	}
}
