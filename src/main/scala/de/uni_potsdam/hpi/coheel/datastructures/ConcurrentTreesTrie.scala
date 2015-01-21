package de.uni_potsdam.hpi.coheel.datastructures

import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory
import com.googlecode.concurrenttrees.radix.node.concrete.voidvalue.VoidValue
import com.googlecode.concurrenttrees.radix.{ConcurrentRadixTree, RadixTree}
import com.googlecode.concurrenttrees.radixinverted.ConcurrentInvertedRadixTree

import scala.collection.JavaConverters._

class ConcurrentTreesTrie extends Trie {

	val rt = new ConcurrentInvertedRadixTree[VoidValue](new DefaultCharArrayNodeFactory)

	def getKeysContainedIn(document: String): Iterator[String] = {
		rt.getKeysContainedIn(document).iterator().asScala.map(_.toString.trim)
	}
	override def add(tokens: String): Unit = {
		rt.put(tokens + " ", VoidValue.SINGLETON)
	}

	override def slidingContains(arr: Array[String], startIndex: Int): Seq[Seq[String]] = {
		???
	}

	override def slidingContains[T](arr: Array[T], toString: (T) => String, startIndex: Int): Seq[Seq[T]] = {
		???
	}

	override def contains(tokens: String): ContainsResult = {
		val tokenString = tokens + " "
		val asEntry = rt.getValueForExactKey(tokenString) != null
//		val asIntermediaNode = rt.getKeysStartingWith(tokenString).iterator().hasNext
		ContainsResult(asEntry, false)//asIntermediaNode)
	}
}
