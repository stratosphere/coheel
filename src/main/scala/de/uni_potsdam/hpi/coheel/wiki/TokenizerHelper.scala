package de.uni_potsdam.hpi.coheel.wiki

import java.io.StringReader

import de.uni_potsdam.hpi.coheel.programs.DataClasses.Link
import de.uni_potsdam.hpi.coheel.util.StanfordPos
import org.apache.flink.shaded.com.google.common.collect.TreeRangeMap
import org.apache.lucene.analysis.en.PorterStemFilter
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.tokenattributes.{CharTermAttribute, FlagsAttribute, OffsetAttribute, TypeAttribute}
import org.apache.lucene.analysis.util.CharArraySet
import org.apache.lucene.util.Version

import scala.collection.mutable

/**
 * Small wrapper around Lucene's tokenizing and stemming.
 */
object TokenizerHelper {

	val STEMMING_DEFAULT = true

	def tokenize(text: String): Array[String] = {
		val tokens = mutable.ArrayBuffer[String]()
		tokenStream(text, STEMMING_DEFAULT) { (charTermAttribute, posAttribute, typeAttribute, flagsAttribute) =>
			tokens += charTermAttribute.toString

		}
		tokens.toArray
	}

	/**
	 * Tokenizes the given text while preserving some of the structure and position indices in
	 *
	 * Use case:
	 * Let `text` be the raw plain text in of an article and `positionInfo` be a dictionary mapping
	 * from positions in the raw text to links occurring at that position.
	 * Now this function tokenizes and stems the plain text and also returns position info with respect to
	 * tokenization.
	 * If before we knew that there was a link at position 63 in the text, we now know there is a link at
	 * index 7 in the token array.
	 */

	var successfulPos = 0
	var unsuccessfulPos = 0
	def tokenizeWithPositionInfo(text: String, positionInfo: TreeRangeMap[Integer, Link]): (Array[String], mutable.Map[Int, Link]) = {
		val tokens = mutable.ArrayBuffer[String]()
		val arrayOffsetToLink = mutable.Map[Int, Link]()

		val posTags = StanfordPos.tagPOS(text)

		tokenStream(text, STEMMING_DEFAULT) { (charTermAttribute, offsetAttribute, _, _) =>
			// add latest token
			tokens += charTermAttribute.toString

			var currentTokenArrayOffset = -1
			val startOffset = offsetAttribute.startOffset()
			// check if we have some position information bundled with the current position
			Option(positionInfo.getEntry(startOffset)) match {
				case Some(entry) =>
					// check, whether a new link started, then build a new offset, use old link offset otherwise
					// last index in the tokens array is the index of the link in the new tokenized output array
					currentTokenArrayOffset = if (currentTokenArrayOffset == -1) tokens.size - 1 else currentTokenArrayOffset
					val range = entry.getKey
					val link  = entry.getValue
					// check whether a pos tags exists for the current word in the link
					posTags.get(startOffset) match {
						case Some(newPosTag) =>
							// build link with new pos tag
							val newLink = link.copy(posTags = link.posTags :+ newPosTag)
							// store it back in position info, so we accumulate all tags and ..
							positionInfo.put(range, newLink)
							// .. store it in the output
							arrayOffsetToLink(currentTokenArrayOffset) = newLink
						case None =>
							// sometimes pos tags do not exist for all tokens, because lucene tokenization and stanford tokenization
							// is different
					}
				case None =>
					// reset the token array offset to indicate, that a link is over
					currentTokenArrayOffset = -1
			}
		}
		(tokens.toArray, arrayOffsetToLink)
	}

	type TokenHandler = (CharTermAttribute, OffsetAttribute, TypeAttribute, FlagsAttribute) => Unit

	private def tokenStream(text: String, stemming: Boolean)(tokenHandler: TokenHandler): Unit = {
		val analyzer = new StandardAnalyzer(Version.LUCENE_48, CharArraySet.EMPTY_SET)
		// implemented following this guide:
		// http://stackoverflow.com/questions/6334692/how-to-use-a-lucene-analyzer-to-tokenize-a-string
		val tokenStream = if (stemming)
			new PorterStemFilter(analyzer.tokenStream(null, new StringReader(text)))
		else
			analyzer.tokenStream(null, new StringReader(text))

		tokenStream.reset()

		val charTermAttribute = tokenStream.addAttribute(classOf[CharTermAttribute])
		val offsetAttribute   = tokenStream.addAttribute(classOf[OffsetAttribute])
		val typeAttribute     = tokenStream.addAttribute(classOf[TypeAttribute])
		val flagAttribute     = tokenStream.addAttribute(classOf[FlagsAttribute])

		while (tokenStream.incrementToken()) {
			tokenHandler(charTermAttribute, offsetAttribute, typeAttribute, flagAttribute)
		}
		tokenStream.end()
		tokenStream.close()
	}

}
