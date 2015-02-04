package de.uni_potsdam.hpi.coheel.datastructures

import java.util

import de.hpi.util.token.TokenSequence
import de.hpi.util.trie.Node
import de.hpi.util.trie.generic.GenericTrieBuilder

class TrieToni extends Trie with FindAllInContainsBased {

	val builder = GenericTrieBuilder.create[String, Int]()
	override def add(tokenString: String): Unit = {
		val split = tokenString.split(' ')
		val splitList = new util.ArrayList[String](split.size)
		var i = 0
		while (i < split.size) {
			splitList.add(split(i))
			i += 1
		}
		builder.insert(new GenericTokenSequence(splitList))
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
		ContainsResult(node.isTerminal, true)
	}
}


class GenericTokenSequence[NodeType](elements: util.ArrayList[NodeType]) extends TokenSequence[NodeType] {


	override def iterator(): java.util.Iterator[NodeType] = elements.iterator()

	override def length(): Int = elements.size
}
