package de.uni_potsdam.hpi.coheel.datastructures

import com.google.common.collect.Lists
import de.hpi.util.token.TokenSequence
import de.hpi.util.trie.Node
import de.hpi.util.trie.generic.GenericTrieBuilder
import scala.collection.JavaConverters._

class TrieToni extends HashTrie {

	val builder = GenericTrieBuilder.create[String, Int]()
	override def add(tokenString: String): Unit = {
		builder.insert(new GenericTokenSequence(tokenString.split(' ').toIterable.asJava))
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
}


class GenericTokenSequence[NodeType](tokens: java.lang.Iterable[NodeType]) extends TokenSequence[NodeType] {

	val elements = Lists.newArrayList(tokens)

	override def iterator(): java.util.Iterator[NodeType] = elements.iterator()

	override def length(): Int = elements.size
}
