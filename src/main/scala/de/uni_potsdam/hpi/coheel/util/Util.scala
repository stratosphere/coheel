package de.uni_potsdam.hpi.coheel.util

import scala.collection.mutable
import scala.util.hashing.MurmurHash3

object Util {

	val CONTEXT_SPREADING_DEFAULT = 25
	/**
	 * Extracts the context from an position in the array.
	 * @param contextSpreading How far the algorithm should look in either side of the word.
	 *                    The final context size
	 * @return Some array with guaranteed size of 2 * contextSpreading + 1, None if that's not possible,
	 *         because the array is to small.
	 */
	def extractContext(a: mutable.ArrayBuffer[String], position: Int, contextSpreading: Int = CONTEXT_SPREADING_DEFAULT): Option[mutable.ArrayBuffer[String]] = {
		if (a.length < 2 * contextSpreading + 1)
			// if the text is not long enough to create a context, we abort, because the context probabilty is not comparable then
			None
		else  {
			// Determine how many words can be included in the context to the left.
			// At most #contextSpreading words are needed.
			val spaceToLeft = Math.min(position, contextSpreading)
			// If not all #contextSpreading words can be taken from the left, some more words have to be
			// taken from the right.
			val spaceToRight = Math.min(a.length - position - 1, contextSpreading)
			val rangeToLeft  = spaceToLeft + (contextSpreading - spaceToRight)
			val rangeToRight = spaceToRight + (contextSpreading - spaceToLeft)

			assert(rangeToLeft + rangeToRight == 2 * contextSpreading)
			Some(a.slice(position - rangeToLeft, position + rangeToRight + 1))
		}
	}

	def id(s: String): String = {
		// make sure, its at least zero
		val hash = MurmurHash3.stringHash(s).toLong + Int.MaxValue + 1
		f"$hash%010d"
	}

}
