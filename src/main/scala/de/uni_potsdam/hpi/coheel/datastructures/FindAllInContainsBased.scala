package de.uni_potsdam.hpi.coheel.datastructures

import scala.collection.mutable

trait FindAllInContainsBased {

	def contains(tokenString: String): ContainsResult

	/**
	 * Same as slidingContains(Array[String], startIndex: Int), but works in arbitrary types.
	 * Needs a conversion function form the type to a string.
	 */
	private def slidingContains[T](arr: Array[T], toString: T => String, startIndex: Int): Seq[Seq[T]] = {
		var result = List[Seq[T]]()
		// vector: immutable list structure with fast append
		var currentCheck = Vector[T](arr(startIndex))
		var containsResult = this.contains(currentCheck.map(toString).mkString(" "))

		var i = 1
		// for each word, go so far until it is no intermediate node anymore
		while (containsResult.asIntermediateNode) {
			// if it is a entry, add to to result list
			if (containsResult.asEntry)
				result ::= currentCheck
			// expand current window, if possible
			while (startIndex + i < arr.size && arr(startIndex + i) == "") {
				i += 1
			}
			if (startIndex + i < arr.size) {
				// append element to the end of the vector
				currentCheck :+= arr(startIndex + i)
				containsResult = this.contains(currentCheck.map(toString).mkString(" "))
				i += 1
			} else {
				// if we reached the end of the text, we need to break manually
				containsResult = ContainsResult(false, false)
			}
		}
		result
	}

	/**
	 * Returns all elements of the trie, starting from a certain offset and going as far as necessary.
	 * @param arr The array to search in.
	 * @param startIndex And the start index.
	 * @return A list of the trie elements matching to the array starting from the start index.
	 */
	private def slidingContains(arr: Array[String], startIndex: Int): Seq[Seq[String]] = {
		slidingContains[String](arr, { s => s }, startIndex)
	}

	def findAllIn(text: String): Iterator[String] = {
		val tokens = text.split(' ')
		val resultSurfaces = mutable.HashSet[String]()

		// each word and its following words must be checked, if it is a surface
		var i = 0
		while (i < tokens.size) {
			val result = slidingContains(tokens, i).map {
				containment => containment.mkString(" ")
			}
			resultSurfaces ++= result
			i += 1
		}
		resultSurfaces.iterator
	}
}
