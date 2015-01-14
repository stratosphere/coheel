package de.uni_potsdam.hpi.coheel.datastructures

class PatriciaTrieWrapper extends TrieLike {

	private [datastructures] def foo(): Int = 1

	val pt = new PatriciaTrie[java.lang.Boolean]()

	override def add(tokens: Seq[String]): Unit = {
		pt.put(tokens.mkString(" "), true)
	}

	override def contains(tokens: Seq[String]): ContainsResult = {
		val tokenString = tokens.mkString(" ")
		val asIntermediateNode = !pt.prefixMap(tokenString).isEmpty
		val asEntry = pt.get(tokenString) != null
		ContainsResult(asEntry, asIntermediateNode)
	}

	def slidingContains(arr: Array[String], startIndex: Int): Seq[Seq[String]] = {
		slidingContains[String](arr, { s => s }, startIndex)
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
}
