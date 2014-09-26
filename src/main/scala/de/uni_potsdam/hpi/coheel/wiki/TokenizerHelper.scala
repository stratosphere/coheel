package de.uni_potsdam.hpi.coheel.wiki

import org.apache.lucene.analysis.en.{PorterStemFilter, EnglishAnalyzer}
import org.apache.lucene.util.Version
import java.io.StringReader
import scala.collection.mutable.ListBuffer
import org.apache.lucene.analysis.tokenattributes.{OffsetAttribute, CharTermAttribute}

/**
 * Small wrapper around Lucene's tokenizing and stemming.
 */
object TokenizerHelper {

	def tokenize(text: String, stemming: Boolean = true): List[String] = {
		val tokens = ListBuffer[String]()
		tokenizeHelper(text, stemming) { (charTermAttribute, _) =>
			tokens += charTermAttribute.toString
		}
		tokens.result()
	}

	case class Token(word: String, startOffset: Int, endOffset: Int)
	def tokenizeWithPositions(text: String, stemming: Boolean = true): List[Token] = {
		val tokens = ListBuffer[Token]()
		tokenizeHelper(text, stemming) { (charTermAttribute, offsetAttribute) =>
			tokens += Token(charTermAttribute.toString, offsetAttribute.startOffset(), offsetAttribute.endOffset())
		}
		tokens.result()
	}

	private def tokenizeHelper(text: String, stemming: Boolean)(tokenHandler: (CharTermAttribute, OffsetAttribute) => Unit): Unit = {
		val analyzer = new EnglishAnalyzer(Version.LUCENE_48)
		// implemented following this guide:
		// http://stackoverflow.com/questions/6334692/how-to-use-a-lucene-analyzer-to-tokenize-a-string
		val tokenStream = if (stemming)
			new PorterStemFilter(analyzer.tokenStream(null, new StringReader(text)))
		else
			analyzer.tokenStream(null, new StringReader(text))

		tokenStream.reset()

		val charTermAttribute = tokenStream.addAttribute(classOf[CharTermAttribute])
		val offsetAttribute   = tokenStream.addAttribute(classOf[OffsetAttribute])

		while (tokenStream.incrementToken()) {
			tokenHandler(charTermAttribute, offsetAttribute)
		}
		analyzer.close()
	}

}
