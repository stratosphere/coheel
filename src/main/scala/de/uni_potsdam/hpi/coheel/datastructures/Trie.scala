package de.uni_potsdam.hpi.coheel.datastructures

import java.util
import java.util.Map

case class ContainsResult(asEntry: Boolean, asIntermediateNode: Boolean)

case class Trie() {

	val rootNode = TrieNode()

	def add(tokens: Seq[String]): Unit = {
		if (tokens.isEmpty)
			throw new RuntimeException("Cannot add empty tokens.")
		rootNode.add(tokens)
	}
	def add(tokenString: String): Unit = add(tokenString.split(' '))

	def contains(tokens: Seq[String]): ContainsResult = {
		if (tokens.isEmpty)
			throw new RuntimeException("Cannot add empty tokens.")
		rootNode.contains(tokens)
	}
	def contains(tokenString: String): ContainsResult = contains(tokenString.split(' '))
}

case class TrieNode() {

	var isEntry = false

	var children: Map[Int, TrieNode] = _

	def add(tokens: Seq[String]): Unit = {
		if (children == null)
			children = new util.HashMap()
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
}
