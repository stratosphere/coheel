package de.uni_potsdam.hpi.coheel.datastructures

case class ContainsResult(asEntry: Boolean, asIntermediateNode: Boolean, prob: Float = Float.NaN)

trait Trie {
	def add(tokenString: String, prob: Float): Unit = add(tokenString)
	def add(tokenString: String): Unit = add(tokenString, Float.NaN)
	def contains(tokenString: String): ContainsResult
	def findAllIn(text: String): Iterator[String]
}

class HashTrie(splitter: String => Array[String] = { s => s.split(' ')}) extends Trie with FindAllInContainsBased {

	var isEntry = false
	var isShortcut = false

	var children: Map[String, HashTrie] = _

	override def add(tokens: String): Unit = {
		if (tokens.isEmpty)
			throw new RuntimeException("Cannot add empty tokens.")
		add(splitter(tokens))
	}

	def add(tokens: Seq[String]): Unit = {
		if (children == null)
			children = Map.empty
		val tokenHead = tokens.head
		if (tokens.tail.isEmpty) {
			children.get(tokenHead) match {
				case None =>
					val newNode = new HashTrie()
					newNode.isEntry = true
					children += (tokenHead -> newNode)
				case Some(trieNode) => trieNode.isEntry = true
			}
		} else {
			children.get(tokenHead) match {
				case None =>
					val newNode = new HashTrie()
					newNode.add(tokens.tail)
					children += (tokenHead -> newNode)
				case Some(trieNode) =>
					trieNode.add(tokens.tail)
			}
		}
	}

	def contains(tokens: String): ContainsResult = {
//		if (tokens.isEmpty)
//			throw new RuntimeException("Cannot add empty tokens.")
		contains(splitter(tokens))
	}

	def contains(tokens: Seq[String]): ContainsResult = {
		// We found the correct node, now check if it is an entry
		if (tokens.isEmpty)
			ContainsResult(isEntry, true, Float.NaN)
		// We reached an early end in the tree (no child node, even though we have more tokens to process)
		else if (children == null)
			ContainsResult(false, false, Float.NaN)
		else {
			children.get(tokens.head) match {
				case None => ContainsResult(false, false, Float.NaN)
				case Some(trieNode) => trieNode.contains(tokens.tail)
			}
		}
	}
}
