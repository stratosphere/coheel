package de.hpi.uni_potsdam.coheel_stratosphere.wiki

import org.apache.lucene.analysis.en.{PorterStemFilter, EnglishAnalyzer}
import org.apache.lucene.util.Version
import java.io.StringReader
import scala.collection.mutable.ListBuffer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute

class TextAnalyzer {

	def analyze(text: String): List[String] = {
		// implemented following this guide:
		// http://stackoverflow.com/questions/6334692/how-to-use-a-lucene-analyzer-to-tokenize-a-string
		val analyzer = new EnglishAnalyzer(Version.LUCENE_48)
		val tokenStream = new PorterStemFilter(analyzer.tokenStream(null, new StringReader(text)))
		tokenStream.reset()
		val tokens = ListBuffer[String]()
		while (tokenStream.incrementToken()) {
			tokens += tokenStream.getAttribute(classOf[CharTermAttribute]).toString
		}
		tokens.result()
	}

}
