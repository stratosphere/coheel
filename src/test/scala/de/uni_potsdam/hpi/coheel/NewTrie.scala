package de.uni_potsdam.hpi.coheel

import de.uni_potsdam.hpi.coheel.datastructures.{ContainsResult, Trie}
import scala.collection.mutable

class NewTrie extends Trie {

	var children = Map[String, NewTrie]()
	var isEntry = false

	override def add(tokenString: String): Unit = {
		add(tokenString.split(' '), 0)
	}
	def add(tokens: Array[String], i: Int): Unit = {
		val head = tokens(i)
		val node = children.get(head) match {
			case Some(existingNode) =>
				existingNode
			case None =>
				val newNode = new NewTrie
				children += head -> newNode
				newNode
		}
		if (i != tokens.size - 1)
			node.add(tokens, i + 1)
		else
			node.isEntry = true
	}

//	override def findAllIn(text: String): Iterable[String] = {
//		findAllIn(text.split(' '))
//	}
//	def findAllIn(tokens: Array[String]): Iterable[String] = {
//		new Iterator[String] {
//			var startIndex = 0
//			var currentOffset = 0
//			var hasNextCalled = false
//
//			override def hasNext: Boolean = {
//				hasNextCalled = true
//
//			}
//
//			override def next(): String = {
//				if (!hasNextCalled) {
//					val resultHasNext = hasNext
//					if (!resultHasNext)
//						throw new Exception("No next element")
//				}
//				hasNextCalled = false
//				var s = ""
//				var i = 0
//				while (i < currentOffset) {
//					s += tokens(i) + " "
//					i += 1
//				}
//				s.trim
//			}
//		}.toIterable
//		var i = 0
//		while (i < tokens.size) {
//			var j
//			i += 1
//		}
//	}

	override def contains(tokenString: String): ContainsResult = {
		contains(tokenString.split(' '))
	}
	def contains(tokens: Array[String]): ContainsResult = {
		var node = this

		var i = 0
		while (i < tokens.size) {
			node.children.get(tokens(i)) match {
				case Some(nextNode) =>
					node = nextNode
				case None =>
					return ContainsResult(false, false)
			}
			i += 1
		}
		ContainsResult(node.isEntry, true)
	}




















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

	override def findAllIn(text: String): Iterable[String] = {
		val tokens = text.split(' ')
		val resultSurfaces = mutable.HashSet[String]()

		// each word and its following words must be checked, if it is a surface
		for (i <- 0 until tokens.size) {
			resultSurfaces ++= slidingContains(tokens, i).map {
				containment => containment.mkString(" ")
			}
		}
		resultSurfaces
	}
}
