package de.uni_potsdam.hpi.coheel.datastructures

import scala.collection.mutable


abstract class NewTrieNode {
	def add(tokens: Array[String], i: Int): NewTrieNode
	def isEntry: Boolean
	def setIsEntry(b: Boolean): NewTrieNode

	def getChild(s: String): Option[NewTrieNode]
}

object EntryZeroNewTrieNode extends ZeroNewTrieNode {
	override def isEntry = true
	override def setIsEntry(b: Boolean): NewTrieNode = {
		if (isEntry == b)
			this
		else
			NoEntryZeroNewTrieNode
	}
}
object NoEntryZeroNewTrieNode extends ZeroNewTrieNode {
	def isEntry: Boolean = false
	override def setIsEntry(b: Boolean): NewTrieNode = {
		if (isEntry == b)
			this
		else
			EntryZeroNewTrieNode
	}
}

abstract class ZeroNewTrieNode extends NewTrieNode {

	override def add(tokens: Array[String], i: Int): NewTrieNode = {
		val isLastToken = i == tokens.size - 1
		val head = tokens(i)
		var newNode: NewTrieNode = if (isLastToken) EntryZeroNewTrieNode else NoEntryZeroNewTrieNode
		if (!isLastToken)
			newNode = newNode.add(tokens, i + 1)
		val resultNode = new OneNewTrieNode(head, newNode)
		resultNode.nodeIsEntry = isEntry
		resultNode
	}

	override def getChild(s: String): Option[NewTrieNode] = None
}

class OneNewTrieNode(key: String, var value: NewTrieNode) extends NewTrieNode {

	var nodeIsEntry: Boolean = false
	override def isEntry: Boolean = nodeIsEntry

	override def setIsEntry(b: Boolean): NewTrieNode = {
		nodeIsEntry = b
		this
	}

	override def add(tokens: Array[String], i: Int): NewTrieNode = {
		val head = tokens(i)
		val isLastToken = i == tokens.size - 1

		var newReturn: MapNewTrieNode = null
		val node = if (head == key) {
			if (isLastToken) value.setIsEntry(true) else value
		} else {
			val newNode = if (isLastToken) EntryZeroNewTrieNode else NoEntryZeroNewTrieNode
			newReturn = new MapNewTrieNode(mutable.Map(key -> value, head -> newNode))
			newReturn.setIsEntry(isEntry)
			newNode
		}
		if (!isLastToken) {
			val tmp = node.add(tokens, i + 1)
			if (tmp != node) {
				if (newReturn == null)
					value = tmp
				else
					newReturn.children += head -> tmp
			}
		}
		if (newReturn == null)
			this
		else
			newReturn
	}

	override def getChild(s: String): Option[NewTrieNode] = {
		if (s == key)
			Some(value)
		else
			None
	}
}

class MapNewTrieNode(var children: mutable.Map[String, NewTrieNode] = mutable.Map()) extends NewTrieNode {

	var nodeIsEntry: Boolean = false

	override def isEntry: Boolean = nodeIsEntry
	override def setIsEntry(b: Boolean): NewTrieNode = {
		nodeIsEntry = b
		this
	}

	override def getChild(s: String) = children.get(s)

	def add(tokens: Array[String], i: Int): NewTrieNode = {
		val head = tokens(i)
		val isLastToken = i == tokens.size - 1

		val node = getChild(head) match {
			case Some(existingNode) =>
				existingNode
			case None =>
				val newNode = if (isLastToken) EntryZeroNewTrieNode else NoEntryZeroNewTrieNode
				children += head -> newNode
				newNode
		}
		if (!isLastToken) {
			val tmp = node.add(tokens, i + 1)
			if (tmp != node)
				children += (head -> tmp)
			this
		}
		else {
			node.setIsEntry(true)
			this
		}
	}

	def contains(tokens: Array[String]): ContainsResult = {
		var node: NewTrieNode = this

		var i = 0
		while (i < tokens.size) {
			node.getChild(tokens(i)) match {
				case Some(nextNode) =>
					node = nextNode
				case None =>
					return ContainsResult(false, false)
			}
			i += 1
		}
		ContainsResult(node.isEntry, true)
	}
}

class NewTrie extends Trie with FindAllInContainsBased {

	val rootNode = new MapNewTrieNode

	override def add(tokenString: String): Unit = {
		rootNode.add(tokenString.split(' '), 0)
	}

	override def contains(tokenString: String): ContainsResult = {
		rootNode.contains(tokenString.split(' '))
	}

	override def findAllIn(text: String): Iterable[String] = {
		findAllIn(text.split(' '))
	}

	def findAllIn(tokens: Array[String]): Iterable[String] = {
		val it = new FindAllInIterator(rootNode, tokens)
		it.toIterable
	}

}

class FindAllInIterator(rootNode: MapNewTrieNode, tokens: Array[String]) extends Iterator[String] {
	var startIndex = 0
	var indexOffset = 0
	var hasNextCalled = false
	val tokenSize = tokens.size

	def currentIndex = startIndex + indexOffset

	var currentNode: NewTrieNode = rootNode
	var lastReturnedNode: NewTrieNode = rootNode

	override def hasNext: Boolean = {
		hasNextCalled = true
		var alreadyReturned = currentNode.isEntry
//		println()
//		println(s"Starting hasNext, $alreadyReturned")
		while (alreadyReturned || !currentNode.isEntry) {
			if (currentIndex >= tokenSize) {
				startIndex += 1
				indexOffset = 0
				currentNode = rootNode
//				println(s"Setting Index-Offset to $indexOffset, Start-Index to $startIndex")
			}
			if (currentIndex >= tokenSize && startIndex >= tokenSize - 1) {
//				println("Returning false")
				return false
			}
			alreadyReturned = false
			currentNode.getChild(tokens(currentIndex)) match {
				case Some(node) =>
					currentNode = node
					indexOffset += 1
//					println(s"Increasing Index-Offset to $indexOffset")
				case _ =>
					currentNode = rootNode
					startIndex += 1
					indexOffset = 0
//					println(s"Setting Index-Offset to $indexOffset, Start-Index to $startIndex")
			}
		}
//		println("Returning true.")
		true
	}

	override def next(): String = {
		if (!hasNextCalled) {
			val resultHasNext = hasNext
			if (!resultHasNext)
				throw new Exception("No next element")
		}
		hasNextCalled = false
		var s = ""
		var i = 0
		while (i < indexOffset) {
			s += tokens(startIndex + i) + " "
			i += 1
		}
//		println(s"Returning >${s.trim}<")
		s.trim
	}
}
