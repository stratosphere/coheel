package de.uni_potsdam.hpi.coheel.wiki

import org.apache.lucene.analysis.Analyzer.TokenStreamComponents
import org.apache.lucene.analysis.standard.{StandardFilter, StandardTokenizer, StandardAnalyzer}
import org.apache.lucene.analysis.util.CharArraySet
import org.apache.lucene.util.Version

import org.apache.lucene.analysis._
import org.apache.lucene.analysis.core.LowerCaseFilter
import org.apache.lucene.analysis.core.StopAnalyzer
import org.apache.lucene.analysis.core.StopFilter
import org.apache.lucene.analysis.util.CharArraySet
import org.apache.lucene.analysis.util.StopwordAnalyzerBase
import org.apache.lucene.util.Version
import java.io.Reader

object WikipediaAnalyzer {
	val STOP_WORDS_SET = StopAnalyzer.ENGLISH_STOP_WORDS_SET
}

/**
 * WikipediaAnalyer, adopted from Lucene's StandardAnalyzer, but with a WikipediaTokenizer.
 */
class WikipediaAnalyzer(matchVersion: Version, stopWords: CharArraySet)
	extends StopwordAnalyzerBase(matchVersion, stopWords) {

	def this(matchVersion: Version) {
		this(matchVersion, WikipediaAnalyzer.STOP_WORDS_SET)
	}

	protected override def createComponents(fieldName: String, reader: Reader): TokenStreamComponents = {
		val src = new WikipediaTokenizer(reader)
		var tok: TokenFilter = new StandardFilter(matchVersion, src)
		tok = new LowerCaseFilter(matchVersion, tok)
		tok = new StopFilter(matchVersion, tok, stopwords)
		new TokenStreamComponents(src, tok) {
			protected override def setReader(reader: Reader) {
				super.setReader(reader)
			}
		}
	}
}

