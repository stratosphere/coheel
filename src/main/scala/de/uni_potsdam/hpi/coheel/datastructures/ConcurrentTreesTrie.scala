package de.uni_potsdam.hpi.coheel.datastructures

import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory
import com.googlecode.concurrenttrees.radix.node.concrete.voidvalue.VoidValue
import com.googlecode.concurrenttrees.radix.{ConcurrentRadixTree, RadixTree}
import com.googlecode.concurrenttrees.radixinverted.ConcurrentInvertedRadixTree

import scala.collection.JavaConverters._

class ConcurrentTreesTrie extends Trie {

	val radixTrie = new ConcurrentInvertedRadixTree[VoidValue](new DefaultCharArrayNodeFactory)

	def getKeysContainedIn(document: String): Iterator[String] = {
		radixTrie.getKeysContainedIn(document).iterator().asScala.map(_.toString.trim)
	}
	override def add(tokens: String): Unit = {
		radixTrie.put(" " + tokens + " ", VoidValue.SINGLETON)
	}

	override def contains(tokens: String): ContainsResult = {
		val tokenString = " " + tokens + " "
		val asEntry = radixTrie.getValueForExactKey(tokenString) != null
		val asIntermediaNode = radixTrie.getKeysStartingWith(tokenString).iterator().hasNext
		ContainsResult(asEntry, asIntermediaNode)
	}

	override def findAllIn(text: String): Iterator[String] = {
		radixTrie.getKeysContainedIn(" " + text + " ").asScala.map(_.toString.trim).toIterator
	}
}
