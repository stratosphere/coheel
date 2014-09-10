package de.uni_potsdam.hpi.coheel

import de.uni_potsdam.hpi.coheel.datastructures.Trie
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TrieTest extends FunSuite {

	def newTrie(): Trie = {
		val trie = new Trie()
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
		val result1 = trie.slidingContains(testSentence, 0)
		val expected1 = Seq("angela", "angela merkel", "angela merkel is german")

		val result2 = trie.slidingContains(testSentence, 1)
		assert(result2.size === 1)
		assert(result2.contains("merkel"))
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
