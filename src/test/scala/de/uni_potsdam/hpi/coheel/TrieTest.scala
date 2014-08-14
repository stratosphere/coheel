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
		assert(trie.contains("angela"))
	}

	test("multiple word queries work") {
		val trie = newTrie()
		trie.add("angela merkel")
		assert(trie.contains("angela merkel"))
	}

	test("only actually added words are considered") {
		val trie = newTrie()
		trie.add("angela merkel")
		assert(!trie.contains("angela"))
	}

	test("multiple adds do not cause harm") {
		val trie = newTrie()
		trie.add("angela merkel")
		trie.add("angela merkel")
		trie.add("angela merkel")
		assert(!trie.contains("angela"))
		assert(trie.contains("angela merkel"))
	}

	test("branching works at every level") {
		val trie = newTrie()
		trie.add("angela dorothea merkel")
		trie.add("angela dorothea kanzler")
		assert(!trie.contains("dorothea"))
		assert(!trie.contains("angela dorothea"))
		assert(trie.contains("angela dorothea merkel"))
	}

	test("can go many levels deep") {
		val trie = newTrie()
		trie.add("ab cd ef gh ij kl")
		assert(trie.contains("ab cd ef gh ij kl"))
	}
}
