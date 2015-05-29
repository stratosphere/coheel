package de.uni_potsdam.hpi.coheel.datastructures

import scala.collection.mutable


abstract class NewTrieNode {
	def add(tokens: Array[String], i: Int, tokenProb: Float): NewTrieNode
	def isEntry: Boolean
	def setIsEntry(b: Boolean): NewTrieNode

	def getChild(s: String): Option[(Float, NewTrieNode)]
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

	override def add(tokens: Array[String], i: Int, tokenProb: Float): NewTrieNode = {
		val isLastToken = i == tokens.length - 1
		val newNodeProb = if (isLastToken) tokenProb else Float.NaN
		val head = tokens(i)
		var newNode: NewTrieNode = if (isLastToken) EntryZeroNewTrieNode else NoEntryZeroNewTrieNode
		if (!isLastToken)
			newNode = newNode.add(tokens, i + 1, tokenProb)
		val resultNode = new OneNewTrieNode(head, newNode, newNodeProb)
		resultNode.nodeIsEntry = isEntry
		resultNode
	}

	override def getChild(s: String): Option[(Float, NewTrieNode)] = None
}

class OneNewTrieNode(val key: String, var value: NewTrieNode, var prob: Float) extends NewTrieNode {

	var nodeIsEntry: Boolean = false
	override def isEntry: Boolean = nodeIsEntry

	override def setIsEntry(b: Boolean): NewTrieNode = {
		nodeIsEntry = b
		this
	}

	override def add(tokens: Array[String], i: Int, tokenProb: Float): NewTrieNode = {
		val head = tokens(i)
		val isLastToken = i == tokens.size - 1
		var newNodeProb = if (isLastToken) tokenProb else Float.NaN

		var newReturn: MapNewTrieNode = null
		val node = if (head == key) {
			newNodeProb = prob
			if (isLastToken) {
				prob = tokenProb
				value.setIsEntry(true)
			} else value
		} else {
			val newNode = if (isLastToken) EntryZeroNewTrieNode else NoEntryZeroNewTrieNode
			newReturn = new MapNewTrieNode(mutable.Map(key -> (prob, value), head -> (newNodeProb, newNode)))
			newReturn.setIsEntry(isEntry)
			newNode
		}
		if (!isLastToken) {
			val tmp = node.add(tokens, i + 1, tokenProb)
			if (tmp != node) {
				if (newReturn == null)
					value = tmp
				else
					newReturn.children += head -> (newNodeProb, tmp)
			}
		}
		if (newReturn == null)
			this
		else
			newReturn
	}

	override def getChild(s: String): Option[(Float, NewTrieNode)] = {
		if (s == key)
			Some(prob, value)
		else
			None
	}
}

class MapNewTrieNode(var children: mutable.Map[String, (Float, NewTrieNode)] = mutable.Map()) extends NewTrieNode {

	var nodeIsEntry: Boolean = false

	override def isEntry: Boolean = nodeIsEntry
	override def setIsEntry(b: Boolean): NewTrieNode = {
		nodeIsEntry = b
		this
	}

	override def getChild(s: String) = children.get(s)

	def add(tokens: Array[String], i: Int, tokenProb: Float): NewTrieNode = {
		val head = tokens(i)
		val isLastToken = i == tokens.size - 1
		var newNodeProb = if (isLastToken) tokenProb else Float.NaN

		val node = getChild(head) match {
			case Some((prob, existingNode)) =>
				if (isLastToken) {
					children += head -> (tokenProb, existingNode)
				} else {
					newNodeProb = prob
				}
				existingNode
			case None =>
				val newNode = if (isLastToken) EntryZeroNewTrieNode else NoEntryZeroNewTrieNode
				children += head -> (newNodeProb, newNode)
				newNode
		}
		if (!isLastToken) {
			val tmp = node.add(tokens, i + 1, tokenProb)
			if (tmp != node)
				children += (head -> (newNodeProb, tmp))
			this
		}
		else {
			node.setIsEntry(true)
			this
		}
	}

	def contains(tokens: Array[String]): ContainsResult = {
		var node: NewTrieNode = this
		var nodeProb: Float = Float.NaN

		var i = 0
		while (i < tokens.size) {
			node.getChild(tokens(i)) match {
				case Some((prob, nextNode)) =>
					node = nextNode
					nodeProb = prob
				case None =>
					return ContainsResult(false, false, Float.NaN)
			}
			i += 1
		}
		ContainsResult(node.isEntry, true, nodeProb)
	}
}

class NewTrie extends Trie {

	val rootNode = new MapNewTrieNode

	override def add(tokenString: String, tokenProb: Float): Unit = {
		rootNode.add(tokenString.split(' '), 0, tokenProb)
	}

	override def contains(tokenString: String): ContainsResult = {
		rootNode.contains(tokenString.split(' '))
	}

	override def findAllIn(text: String): Iterator[String] = {
		findAllIn(text.split(' ').toBuffer)
	}

	def findAllIn(tokens: mutable.Buffer[String]): Iterator[String] = {
		new FindAllInIterator(rootNode, tokens, { t: TrieHit => t.s })
	}
	def findAllInWithTrieHit(tokens: mutable.Buffer[String]): Iterator[TrieHit] = {
		new FindAllInIterator(rootNode, tokens, identity)
	}
	def findAllInWithProbs(text: String): Iterator[(String, Float)] = {
		findAllInWithProbs(text.split(' ').toBuffer)
	}
	def findAllInWithProbs(tokens: mutable.Buffer[String]): Iterator[(String, Float)] = {
		new FindAllInIterator(rootNode, tokens, { t: TrieHit => (t.s, t.prob) })
	}


	override def toString: String = {
		val nodeStack = mutable.Stack[(Int, String, NewTrieNode, Float)]()
		nodeStack.push((0, "", rootNode, Float.NaN))

		val sb = new mutable.StringBuilder()

		while (nodeStack.nonEmpty) {
			val (level, text, node, prob) = nodeStack.pop()
			sb.append(" " * (level * 4) + s"--$prob--> $text\n")
			node match {
				case m: MapNewTrieNode =>
					m.children.toList.sortBy(_._1).foreach { case (nextText, (nextProb, nextNode)) =>
						nodeStack.push((level + 1, nextText, nextNode, nextProb))
					}
				case o: OneNewTrieNode =>
					nodeStack.push((level + 1, o.key, o.value, o.prob))
				case z: ZeroNewTrieNode =>
			}
		}
		sb.toString()
	}

}

case class TrieHit(s: String, prob: Float, startIndex: Int, offset: Int)
class FindAllInIterator[T](rootNode: MapNewTrieNode, tokens: mutable.Buffer[String], fun: TrieHit => T) extends Iterator[T] {

	val alreadySeen = mutable.Set[String]()
	var startIndex = 0
	var indexOffset = 0
	var hasNextCalled = false
	val tokenSize = tokens.length

	def currentIndex = startIndex + indexOffset

	var currentProb = Float.NaN

	var currentNode: NewTrieNode = rootNode
	var lastReturnedNode: NewTrieNode = rootNode

	override def hasNext: Boolean = {
		hasNextCalled = true
		var alreadyReturned = currentNode.isEntry
		while (alreadyReturned || !currentNode.isEntry || alreadySeen.contains(buildCurrentTokenString())) {
			alreadyReturned = false
			if (currentIndex >= tokenSize) {
				startIndex += 1
				indexOffset = 0
				currentNode = rootNode
			}
			if (currentIndex >= tokenSize && startIndex >= tokenSize - 1) {
				return false
			}
			currentNode.getChild(tokens(currentIndex)) match {
				case Some((prob, node)) =>
					currentNode = node
					currentProb = prob
					indexOffset += 1
					while (currentIndex < tokenSize && tokens(currentIndex).isEmpty)
						indexOffset += 1
				case _ =>
					currentNode = rootNode
					startIndex += 1
					indexOffset = 0
			}
		}
		true
	}

	override def next(): T = {
		if (!hasNextCalled) {
			val resultHasNext = hasNext
			if (!resultHasNext)
				throw new Exception("No next element")
		}
		hasNextCalled = false
		val res = buildCurrentTokenString()
		alreadySeen += res
		fun(TrieHit(res, currentProb, startIndex, indexOffset))
	}

	private def buildCurrentTokenString(): String = {
		var s = ""
		var i = 0
		while (i < indexOffset) {
			val nextToken = tokens(startIndex + i)
			if (nextToken.nonEmpty)
				s += tokens(startIndex + i) + " "
			i += 1
		}
		s.trim

	}
}
