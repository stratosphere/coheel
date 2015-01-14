package de.uni_potsdam.hpi.coheel.datastructures

import org.ahocorasick.trie.Trie

class AhoCorasickTrie extends TrieLike {

	val trie: Trie = new Trie().onlyWholeWords()

	override def add(tokenString: String): Unit = {
		trie.addKeyword(tokenString)
	}

	override def contains(tokenString: String): ContainsResult = {
		val result = trie.parseText(tokenString)
		ContainsResult(result.size > 0, false)
	}

	// not implemented
	override def slidingContains(arr: Array[String], startIndex: Int): Seq[Seq[String]] = ???
	override def slidingContains[T](arr: Array[T], toString: (T) => String, startIndex: Int): Seq[Seq[T]] = ???
}
