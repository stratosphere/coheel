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
		val analyzer = new EnglishAnalyzer(Version.LUCENE_48)
		// implemented following this guide:
		// http://stackoverflow.com/questions/6334692/how-to-use-a-lucene-analyzer-to-tokenize-a-string
		val tokenStream = if (stemming)
			new PorterStemFilter(analyzer.tokenStream(null, new StringReader(text)))
		else
			analyzer.tokenStream(null, new StringReader(text))

		tokenStream.reset()
		val tokens = ListBuffer[String]()

		val charTermAttribute = tokenStream.addAttribute(classOf[CharTermAttribute])
		val offsetAttribute   = tokenStream.addAttribute(classOf[OffsetAttribute])
		while (tokenStream.incrementToken()) {
			tokens += charTermAttribute.toString
			println(offsetAttribute.startOffset())
			println(offsetAttribute.endOffset())
		}
		analyzer.close()
		tokens.result()
	}

}
