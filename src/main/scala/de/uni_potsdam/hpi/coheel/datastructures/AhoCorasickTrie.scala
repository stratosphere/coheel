package de.uni_potsdam.hpi.coheel.datastructures

import org.arabidopsis.ahocorasick.AhoCorasick

class AhoCorasickTrie extends Trie {

	val trie = new AhoCorasick

	var alreadyPrepared = false

	override def add(tokenString: String): Unit = {
		trie.add(tokenString)
	}

	override def contains(tokenString: String): ContainsResult = {
		if (!alreadyPrepared) {
			trie.prepare()
			alreadyPrepared = true
		}
		val result = trie.completeSearch(tokenString, false, false)
		ContainsResult(result.size > 0, false)
	}

	// not implemented
	override def slidingContains(arr: Array[String], startIndex: Int): Seq[Seq[String]] = ???
	override def slidingContains[T](arr: Array[T], toString: (T) => String, startIndex: Int): Seq[Seq[T]] = ???
}
