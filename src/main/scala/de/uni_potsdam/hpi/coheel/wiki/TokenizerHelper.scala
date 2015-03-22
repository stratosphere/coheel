package de.uni_potsdam.hpi.coheel.wiki

import java.io.StringReader

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
		tokenizeHelper(text, STEMMING_DEFAULT) { (charTermAttribute, posAttribute, typeAttribute, flagsAttribute) =>
			tokens += charTermAttribute.toString

		}
		tokens.toArray
	}

	def tokenizeWithPositionInfo[T](text: String, positionInfo: mutable.Map[Int, T]): (Array[String], mutable.Map[Int, T]) = {
		val tokens = mutable.ArrayBuffer[String]()
		val arrayOffsetToInfo = mutable.Map[Int, T]()

		tokenizeHelper(text, STEMMING_DEFAULT) { (charTermAttribute, posAttribute, typeAttribute, flagsAttribute) =>
			tokens += charTermAttribute.toString
			positionInfo.get(posAttribute.startOffset()) match {
				case Some(info) =>
					// last index in the tokens array is the index of the information in the new tokenized output array
					arrayOffsetToInfo(tokens.size - 1) = info
				case None =>
			}
		}
		(tokens.toArray, arrayOffsetToInfo)
	}

	type TokenHandler = (CharTermAttribute, OffsetAttribute, TypeAttribute, FlagsAttribute) => Unit

	private def tokenizeHelper(text: String, stemming: Boolean)(tokenHandler: TokenHandler): Unit = {
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
