package de.uni_potsdam.hpi.coheel.datastructures

import com.google.common.collect.Lists
import de.hpi.util.token.TokenSequence
import de.hpi.util.trie.Node
import de.hpi.util.trie.generic.GenericTrieBuilder
import scala.collection.JavaConverters._
import scala.collection.mutable

class TrieToni extends Trie {

	val builder = GenericTrieBuilder.create[String, Int]()
	override def add(tokenString: String): Unit = {
		val split = tokenString.split(' ')
		builder.insert(new GenericTokenSequence(split.toIterable.asJava))
	}

	var trie: de.hpi.util.trie.Trie[String, Int] = null
	override def contains(tokenString: String): ContainsResult = {
		if (trie == null)
			trie = builder.trie()

		val tokens = tokenString.split(' ')
		var node: Node[String, Int] = null

		node = trie
		tokens.foreach { token =>
			node = node.get(token)
			if (node == null) {
				return ContainsResult(false, false)
			}
		}
		ContainsResult(node.isLeaf, true)
	}

	/**
	 * Same as slidingContains(Array[String], startIndex: Int), but works in arbitrary types.
	 * Needs a conversion function form the type to a string.
	 */
	private def slidingContains[T](arr: Array[T], toString: T => String, startIndex: Int): Seq[Seq[T]] = {
		var result = List[Seq[T]]()
		// vector: immutable list structure with fast append
		var currentCheck = Vector[T](arr(startIndex))
		var containsResult = this.contains(currentCheck.map(toString).mkString(" "))

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
				containsResult = this.contains(currentCheck.map(toString).mkString(" "))
				i += 1
			} else {
				// if we reached the end of the text, we need to break manually
				containsResult = ContainsResult(false, false)
			}
		}
		result
	}

	/**
	 * Returns all elements of the trie, starting from a certain offset and going as far as necessary.
	 * @param arr The array to search in.
	 * @param startIndex And the start index.
	 * @return A list of the trie elements matching to the array starting from the start index.
	 */
	private def slidingContains(arr: Array[String], startIndex: Int): Seq[Seq[String]] = {
		slidingContains[String](arr, { s => s }, startIndex)
	}

	override def findAllIn(text: String): Iterable[String] = {
		val tokens = text.split(' ')
		val resultSurfaces = mutable.HashSet[String]()

		// each word and its following words must be checked, if it is a surface
		for (i <- 0 until tokens.size) {
			resultSurfaces ++= slidingContains(tokens, i).map {
				containment => containment.mkString(" ")
			}
		}
		resultSurfaces
	}
}


class GenericTokenSequence[NodeType](tokens: java.lang.Iterable[NodeType]) extends TokenSequence[NodeType] {

	val elements = Lists.newArrayList(tokens)

	override def iterator(): java.util.Iterator[NodeType] = elements.iterator()

	override def length(): Int = elements.size
}
