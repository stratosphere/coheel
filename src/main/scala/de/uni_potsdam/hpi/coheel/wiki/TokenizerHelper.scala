package de.uni_potsdam.hpi.coheel.wiki

import java.io.StringReader

import de.uni_potsdam.hpi.coheel.programs.DataClasses.Link
import edu.stanford.nlp.ling.{CoreLabel, HasWord, TaggedWord}
import edu.stanford.nlp.process.DocumentPreprocessor
import edu.stanford.nlp.process.PTBTokenizer.PTBTokenizerFactory
import edu.stanford.nlp.tagger.maxent.MaxentTagger
import org.apache.flink.shaded.com.google.common.collect.TreeRangeMap
import org.tartarus.snowball.ext.PorterStemmer

import scala.collection.JavaConverters._
import scala.collection.mutable
/**
 * Small wrapper around Lucene's tokenizing and stemming.
 */
object TokenizerHelper {

	val STEMMING_DEFAULT = true

	def tokenize(text: String): Array[String] = {
		val tokens = mutable.ArrayBuffer[String]()
		tokenStream(text, STEMMING_DEFAULT).foreach { sent =>
			sent.asScala.foreach { token =>
				tokens += token.word()
			}
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
	def tokenizeWithPositionInfo(text: String, positionInfo: TreeRangeMap[Integer, Link]): KeepLinkTokenizer = {
		// method object for translating the link annotations in the full text to link annotations
		// for each token
		val tokenizer = new KeepLinkTokenizer(positionInfo)

		val rawTokens = tokenStream(text, STEMMING_DEFAULT)
		rawTokens.foreach(tokenizer.processSentence)
		tokenizer
	}

	val p = "\\p{Punct}+".r
	private def tokenStream(text: String, stemming: Boolean): Iterator[java.util.List[HasWord]] = {
		val textReader = new StringReader(text)
		val stemmer = new PorterStemmer()

		val prep = new DocumentPreprocessor(textReader)
		val tokenizer = PTBTokenizerFactory.newCoreLabelTokenizerFactory("normalizeParentheses=false,normalizeOtherBrackets=false,untokenizable=noneKeep")
		prep.setTokenizerFactory(tokenizer)
		val sentences = prep.iterator().asScala
		val stemmedSentences = sentences.map { sent =>
			var i = 0
			while (i < sent.size()) {
				val token = sent.get(i)
				stemmer.setCurrent(token.word())
				stemmer.stem()
				val stemmedWord = stemmer.getCurrent.toLowerCase.trim
				if (p.unapplySeq(stemmedWord).isEmpty) {
					token.setWord(stemmedWord)
					i += 1
				} else {
					sent.remove(i)
				}
			}
			sent
		}
		stemmedSentences
	}
}
