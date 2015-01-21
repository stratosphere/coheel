package de.uni_potsdam.hpi.coheel.datastructures

import org.arabidopsis.ahocorasick.AhoCorasick
import scala.collection.JavaConverters._

class AhoCorasickTrie extends Trie {

	val trie = new AhoCorasick()

	var alreadyPrepared = false

	override def add(tokenString: String): Unit = {
		trie.add(tokenString)
	}

	override def contains(tokenString: String): ContainsResult = {
		if (!alreadyPrepared) {
			trie.prepare()
			alreadyPrepared = true
		}
		val progressiveSearch = trie.progressiveSearch(tokenString).asScala
		val asIntermediate = progressiveSearch.hasNext
		val result = progressiveSearch.exists { result =>
			result.getOutputs.asScala.map(_.asInstanceOf[String]).contains(tokenString)
		}
		ContainsResult(result, asIntermediate)
	}

	override def findAllIn(text: String): Iterable[String] = {
		if (!alreadyPrepared) {
			trie.prepare()
			alreadyPrepared = true
		}
		val res = trie.progressiveSearch(text).asScala.flatMap { result =>
			result.getOutputs.asScala
		}.map(_.asInstanceOf[String]).toList
		res
	}
}
