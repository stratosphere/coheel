package de.uni_potsdam.hpi.coheel.datastructures

import java.util
import java.util.Map

import scala.collection.mutable.ArrayBuffer

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

	var children: ArrayBuffer[(String, TrieNode)] = _

	def add(tokens: Seq[String]): Unit = {
		if (children == null)
			children = new ArrayBuffer[(String, TrieNode)]()
		if (tokens.tail.isEmpty) {
			children.find { x => x._1 == tokens.head } match {
				case None =>
					val newNode = TrieNode()
					newNode.isEntry = true
					children.+=((tokens.head, newNode))
				case Some((_, trieNode)) => trieNode.isEntry = true
			}
		}
		else {
			children.find { x => x._1 == tokens.head } match {
				case None =>
					val newNode = TrieNode()
					newNode.add(tokens.tail)
					children.+=((tokens.head, newNode))
				case Some((_, trieNode)) =>
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
			children.find { x => x._1 == tokens.head } match {
				case None => false
				case Some((_, trieNode)) => trieNode.contains(tokens.tail)
			}
		}
	}
}
