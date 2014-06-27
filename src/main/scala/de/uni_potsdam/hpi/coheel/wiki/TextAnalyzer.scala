package de.uni_potsdam.hpi.coheel.wiki

import org.apache.lucene.analysis.en.{PorterStemFilter, EnglishAnalyzer}
import org.apache.lucene.util.Version
import java.io.StringReader
import scala.collection.mutable.ListBuffer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute

/**
 * Small wrapper around Lucene's tokenizing and stemming.
 */
class TextAnalyzer {

	def tokenize(text: String, stemming: Boolean = true): List[String] = {
		// implemented following this guide:
		// http://stackoverflow.com/questions/6334692/how-to-use-a-lucene-analyzer-to-tokenize-a-string
		val analyzer = new EnglishAnalyzer(Version.LUCENE_48)
		val tokenStream = if (stemming)
			new PorterStemFilter(analyzer.tokenStream(null, new StringReader(text)))
		else
			analyzer.tokenStream(null, new StringReader(text))

		tokenStream.reset()
		val tokens = ListBuffer[String]()
		while (tokenStream.incrementToken()) {
			tokens += tokenStream.getAttribute(classOf[CharTermAttribute]).toString
		}
		tokens.result()
	}

}
