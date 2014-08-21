package de.uni_potsdam.hpi.coheel.datastructures

import java.util
import java.util.Map

case class Trie() {

	val rootNode = TrieNode()

	def add(tokens: Seq[String]): Unit = {
		if (tokens.isEmpty)
			throw new RuntimeException("Cannot add empty tokens.")
		rootNode.add(tokens)
	}
	def add(tokenString: String): Unit = add(tokenString.split(' '))

	def contains(tokens: Seq[String]): Boolean = {
		if (tokens.isEmpty)
			throw new RuntimeException("Cannot add empty tokens.")
		rootNode.contains(tokens)
	}
	def contains(tokenString: String): Boolean = contains(tokenString.split(' '))
}

case class TrieNode() {
	var isEntry = false

	var children: Map[String, TrieNode] = _

	def add(tokens: Seq[String]): Unit = {
		if (children == null)
			children = new util.HashMap()
		if (tokens.tail.isEmpty) {
			children.get(tokens.head) match {
				case null =>
					val newNode = TrieNode()
					newNode.isEntry = true
					children.put(tokens.head, newNode)
				case trieNode => trieNode.isEntry = true
			}
		}
		else {
			children.get(tokens.head) match {
				case null =>
					val newNode = TrieNode()
					newNode.add(tokens.tail)
					children.put(tokens.head, newNode)
				case trieNode =>
					trieNode.add(tokens.tail)
			}
		}
	}

	def contains(tokens: Seq[String]): Boolean = {
		if (tokens.isEmpty)
			isEntry
		else if (children == null)
			false
		else {
			children.get(tokens.head) match {
				case null => false
				case trieNode => trieNode.contains(tokens.tail)
			}
		}
	}
}
