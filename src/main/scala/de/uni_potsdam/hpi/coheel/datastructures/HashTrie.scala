package de.uni_potsdam.hpi.coheel.datastructures

case class ContainsResult(asEntry: Boolean, asIntermediateNode: Boolean)

trait TrieLike {
	def add(tokenString: String): Unit
	def contains(tokenString: String): ContainsResult
	def slidingContains(arr: Array[String], startIndex: Int): Seq[Seq[String]]
	def slidingContains[T](arr: Array[T], toString: T => String, startIndex: Int): Seq[Seq[T]]
}

case class HashTrie() extends TrieLike {

	val rootNode = HashTrieNode()

	def add(tokens: String): Unit = {
		if (tokens.isEmpty)
			throw new RuntimeException("Cannot add empty tokens.")
		rootNode.add(tokens.split(' '))
	}

	def contains(tokens: String): ContainsResult = {
		if (tokens.isEmpty)
			throw new RuntimeException("Cannot add empty tokens.")
		rootNode.contains(tokens.split(' '))
	}

	/**
	 * Returns all elements of the trie, starting from a certain offset and going as far as necessary.
	 * @param arr The array to search in.
	 * @param startIndex And the start index.
	 * @return A list of the trie elements matching to the array starting from the start index.
	 */
	def slidingContains(arr: Array[String], startIndex: Int): Seq[Seq[String]] = {
		rootNode.slidingContains(arr, startIndex)
	}

	/**
	 * Same as slidingContains(Array[String], startIndex: Int), but works in arbitrary types.
	 * Needs a conversion function form the type to a string.
	 */
	def slidingContains[T](arr: Array[T], toString: T => String, startIndex: Int): Seq[Seq[T]] = {
		rootNode.slidingContains(arr, toString, startIndex)
	}
}

case class HashTrieNode() {

	var isEntry = false

	var children: Map[Int, HashTrieNode] = _
//	var children: Int2ReferenceMap[TrieNode] = _
//	var children: Int2ReferenceMap[TrieNode] = _
//	var children: TIntObjectHashMap[TrieNode] = _

	def add(tokens: Seq[String]): Unit = {
		if (children == null)
//			children = new TIntObjectHashMap[TrieNode]()
			children = Map.empty
		if (tokens.tail.isEmpty) {
			children.get(tokens.head.hashCode) match {
				case None =>
					val newNode = HashTrieNode()
					newNode.isEntry = true
					children += (tokens.head.hashCode -> newNode)
				case Some(trieNode) => trieNode.isEntry = true
			}
		}
		else {
			children.get(tokens.head.hashCode) match {
				case None =>
					val newNode = HashTrieNode()
					newNode.add(tokens.tail)
					children += (tokens.head.hashCode -> newNode)
				case Some(trieNode) =>
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
				case None => ContainsResult(false, false)
				case Some(trieNode) => trieNode.contains(tokens.tail)
			}
		}
	}

	def slidingContains[T](arr: Array[T], toString: T => String, startIndex: Int): Seq[Seq[T]] = {
		var result = List[Seq[T]]()
		// vector: immutable list structure with fast append
		var currentCheck = Vector[T](arr(startIndex))
		var containsResult = this.contains(currentCheck.map(toString))

		var i = 1
		// for each word, go so far until it is no intermediate node anymore
		while (containsResult.asIntermediateNode) {
			// if it is a entry, add to to result list
			if (containsResult.asEntry)
				result ::= currentCheck
			// expand current window, if possible
			if (startIndex + i < arr.size) {
				// append element to the end of the vector
				currentCheck :+= arr(startIndex + i)
				containsResult = this.contains(currentCheck.map(toString))
				i += 1
			} else {
				// if we reached the end of the text, we need to break manually
				containsResult = ContainsResult(false, false)
			}
		}
		result
	}

	def slidingContains(arr: Array[String], startIndex: Int): Seq[Seq[String]] = {
		slidingContains[String](arr, { s => s }, startIndex)
	}
}
