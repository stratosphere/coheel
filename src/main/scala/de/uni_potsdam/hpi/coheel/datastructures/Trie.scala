package de.uni_potsdam.hpi.coheel.datastructures

import scala.collection.mutable.Map

case class Trie(value: String, children: List[Trie]) {

	val rootNode = TrieNode(Map())

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
				case Some(trieNode) => trieNode.isEntry = true
				case None =>
					val newNode = TrieNode(Map())
					newNode.isEntry = true
					children.put(tokens.head, newNode)
			}
		}
		else {
			children.get(tokens.head) match {
				case Some(trieNode) =>
					trieNode.add(tokens.tail)
				case None =>
					val newNode = TrieNode(Map())
					newNode.add(tokens.tail)
					children.put(tokens.head, newNode)
			}
		}
	}

	def contains(tokens: Seq[String]): Boolean = {
		if (tokens.isEmpty)
			isEntry
		else {
			children.get(tokens.head) match {
				case Some(trieNode) => trieNode.contains(tokens.tail)
				case None => false
			}
		}
	}
}
