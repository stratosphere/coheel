package de.uni_potsdam.hpi.coheel.datastructures

import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory
import com.googlecode.concurrenttrees.radix.node.concrete.voidvalue.VoidValue
import com.googlecode.concurrenttrees.radix.{ConcurrentRadixTree, RadixTree}
import com.googlecode.concurrenttrees.radixinverted.ConcurrentInvertedRadixTree
import it.unimi.dsi.fastutil.ints.Int2ReferenceMap
import org.apache.commons.collections4.trie.PatriciaTrie

case class ContainsResult(asEntry: Boolean, asIntermediateNode: Boolean)

trait TrieLike {
	def add(tokens: Seq[String]): Unit
	def add(tokenString: String): Unit = add(tokenString.split(' '))
	def contains(tokens: Seq[String]): ContainsResult
	def contains(tokenString: String): ContainsResult = contains(tokenString.split(' '))
	def slidingContains(arr: Array[String], startIndex: Int): Seq[Seq[String]]
	def slidingContains[T](arr: Array[T], toString: T => String, startIndex: Int): Seq[Seq[T]]
}

class ConcurrentTreesWrapper extends TrieLike {

	val rt = new ConcurrentInvertedRadixTree[VoidValue](new DefaultCharArrayNodeFactory)
	override def add(tokens: Seq[String]): Unit = {
		rt.put(tokens.mkString(" "), VoidValue.SINGLETON)
	}

	override def slidingContains(arr: Array[String], startIndex: Int): Seq[Seq[String]] = {
		throw new RuntimeException("FOOBAR")
	}

	override def slidingContains[T](arr: Array[T], toString: (T) => String, startIndex: Int): Seq[Seq[T]] = {
		throw new RuntimeException("FOOBAR")
	}

	override def contains(tokens: Seq[String]): ContainsResult = {
		val tokenString = tokens.mkString(" ")
		val asEntry = rt.getValueForExactKey(tokenString) != null
		val asIntermediaNode = rt.getKeysStartingWith(tokenString).iterator().hasNext
		ContainsResult(asEntry, asIntermediaNode)
	}
}

class PatriciaTrieWrapper extends TrieLike {

	private [datastructures] def foo(): Int = 1

	val pt = new PatriciaTrie[java.lang.Boolean]()

	override def add(tokens: Seq[String]): Unit = {
		pt.put(tokens.mkString(" "), true)
	}

	override def contains(tokens: Seq[String]): ContainsResult = {
		val tokenString = tokens.mkString(" ")
		val asIntermediateNode = !pt.prefixMap(tokenString).isEmpty
		val asEntry = pt.get(tokenString) != null
		ContainsResult(asEntry, asIntermediateNode)
	}

	def slidingContains(arr: Array[String], startIndex: Int): Seq[Seq[String]] = {
		slidingContains[String](arr, { s => s }, startIndex)
	}
	def slidingContains[T](arr: Array[T], toString: T => String, startIndex: Int): Seq[Seq[T]] = {
		var result = List[Seq[T]]()
		// vector: immutable list structure with fast append
		var currentCheck = Vector[T](arr(startIndex))
		var containsResult = this.contains(currentCheck.map(toString))

		var i = 1
		// for each word, go so far until it is no intermediate node anymore
		while (containsResult.asIntermediateNode) {
			// if it is a entry, add to to result list
			if (containsResult.asEntry)
				result ::= currentCheck
			// expand current window, if possible
			if (startIndex + i < arr.size) {
				// append element to the end of the vector
				currentCheck :+= arr(startIndex + i)
				containsResult = this.contains(currentCheck.map(toString))
				i += 1
			} else {
				// if we reached the end of the text, we need to break manually
				containsResult = ContainsResult(false, false)
			}
		}
		result

	}
}

case class Trie() extends TrieLike {

	val rootNode = TrieNode()

	def add(tokens: Seq[String]): Unit = {
		if (tokens.isEmpty)
			throw new RuntimeException("Cannot add empty tokens.")
		rootNode.add(tokens)
	}

	def contains(tokens: Seq[String]): ContainsResult = {
		if (tokens.isEmpty)
			throw new RuntimeException("Cannot add empty tokens.")
		rootNode.contains(tokens)
	}

	/**
	 * Returns all elements of the trie, starting from a certain offset and going as far as necessary.
	 * @param arr The array to search in.
	 * @param startIndex And the start index.
	 * @return A list of the trie elements matching to the array starting from the start index.
	 */
	def slidingContains(arr: Array[String], startIndex: Int): Seq[Seq[String]] = {
		rootNode.slidingContains(arr, startIndex)
	}

	/**
	 * Same as slidingContains(Array[String], startIndex: Int), but works in arbitrary types.
	 * Needs a conversion function form the type to a string.
	 */
	def slidingContains[T](arr: Array[T], toString: T => String, startIndex: Int): Seq[Seq[T]] = {
		rootNode.slidingContains(arr, toString, startIndex)
	}
}

case class TrieNode() {

	var isEntry = false

	var children: Int2ReferenceMap[TrieNode] = _
//	var children: TIntObjectHashMap[TrieNode] = _

	def add(tokens: Seq[String]): Unit = {
		if (children == null)
//			children = new TIntObjectHashMap[TrieNode]()
			children = new it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap[TrieNode]()
		if (tokens.tail.isEmpty) {
			children.get(tokens.head.hashCode) match {
				case null =>
					val newNode = TrieNode()
					newNode.isEntry = true
					children.put(tokens.head.hashCode, newNode)
				case trieNode => trieNode.isEntry = true
			}
		}
		else {
			children.get(tokens.head.hashCode) match {
				case null =>
					val newNode = TrieNode()
					newNode.add(tokens.tail)
					children.put(tokens.head.hashCode, newNode)
				case trieNode =>
					trieNode.add(tokens.tail)
			}
		}
	}

	def contains(tokens: Seq[String]): ContainsResult = {
		// We found the correct node, now check if it is an entry
		if (tokens.isEmpty)
			ContainsResult(isEntry, true)
		// We reached an early end in the tree (no child node, even though we have more tokens to process)
		else if (children == null)
			ContainsResult(false, false)
		else {
			children.get(tokens.head.hashCode) match {
				case null => ContainsResult(false, false)
				case trieNode => trieNode.contains(tokens.tail)
			}
		}
	}

	def slidingContains[T](arr: Array[T], toString: T => String, startIndex: Int): Seq[Seq[T]] = {
		var result = List[Seq[T]]()
		// vector: immutable list structure with fast append
		var currentCheck = Vector[T](arr(startIndex))
		var containsResult = this.contains(currentCheck.map(toString))

		var i = 1
		// for each word, go so far until it is no intermediate node anymore
		while (containsResult.asIntermediateNode) {
			// if it is a entry, add to to result list
			if (containsResult.asEntry)
				result ::= currentCheck
			// expand current window, if possible
			if (startIndex + i < arr.size) {
				// append element to the end of the vector
				currentCheck :+= arr(startIndex + i)
				containsResult = this.contains(currentCheck.map(toString))
				i += 1
			} else {
				// if we reached the end of the text, we need to break manually
				containsResult = ContainsResult(false, false)
			}
		}
		result
	}

	def slidingContains(arr: Array[String], startIndex: Int): Seq[Seq[String]] = {
		slidingContains[String](arr, { s => s }, startIndex)
	}
}
