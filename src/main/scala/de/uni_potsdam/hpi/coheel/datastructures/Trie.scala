package de.uni_potsdam.hpi.coheel.datastructures

import java.util
import java.util.Map

case class Trie() {

	val rootNode = TrieNode(new util.HashMap())

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

case class TrieNode(children: Map[String, TrieNode]) {
	var isEntry = false

	def add(tokens: Seq[String]): Unit = {
		if (tokens.tail.isEmpty) {
			children.get(tokens.head) match {
				case null =>
					val newNode = TrieNode(new util.HashMap())
					newNode.isEntry = true
					children.put(tokens.head, newNode)
				case trieNode => trieNode.isEntry = true
			}
		}
		else {
			children.get(tokens.head) match {
				case null =>
					val newNode = TrieNode(new util.HashMap())
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
		else {
			children.get(tokens.head) match {
				case null => false
				case trieNode => trieNode.contains(tokens.tail)
			}
		}
	}
}
