package de.uni_potsdam.hpi.coheel.wiki

import java.io.StringReader
import java.util.regex.Pattern

import de.uni_potsdam.hpi.coheel.programs.DataClasses.Link
import de.uni_potsdam.hpi.coheel.util.{Timer, StanfordPos}
import edu.stanford.nlp.ling.HasWord
import edu.stanford.nlp.process.DocumentPreprocessor
import edu.stanford.nlp.process.PTBTokenizer.PTBTokenizerFactory
import edu.stanford.nlp.tagger.maxent.MaxentTagger
import org.apache.flink.shaded.com.google.common.collect.TreeRangeMap
import org.apache.lucene.analysis.en.PorterStemFilter
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.tokenattributes.{CharTermAttribute, FlagsAttribute, OffsetAttribute, TypeAttribute}
import org.apache.lucene.analysis.util.CharArraySet
import org.apache.lucene.util.Version
import org.tartarus.snowball.ext.PorterStemmer
import scala.collection.JavaConverters._

import scala.collection.mutable
/**
 * Small wrapper around Lucene's tokenizing and stemming.
 */
object TokenizerHelper {

	val STEMMING_DEFAULT = true
	val modelName = "edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger"
	val tagger = new MaxentTagger(modelName)

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

	var successfulPos = 0
	var unsuccessfulPos = 0
	def tokenizeWithPositionInfo(text: String, positionInfo: TreeRangeMap[Integer, Link]): (Array[String], mutable.Map[Int, Link]) = {
		val tokens = mutable.ArrayBuffer[String]()
		val arrayOffsetToLink = mutable.Map[Int, Link]()

		var currentTokenArrayIndex = 0
		var currentLink: Link = null

		val rawTokens = tokenStream(text, STEMMING_DEFAULT)
		rawTokens.foreach { sent =>
			val sentenceTags = StanfordPos.tagSentence(sent)
			sentenceTags.foreach { token =>
				tokens += token.word()
				val startOffset = token.beginPosition()
				// check if we have some position information bundled with the current position
				Option(positionInfo.getEntry(startOffset)) match {
					case Some(entry) =>
						val range = entry.getKey
						val link  = entry.getValue
						// check, whether a new link started, then build a new offset, use old link offset otherwise
						// last index in the tokens array is the index of the link in the new tokenized output array
						if (currentLink == null || currentLink.fullId != link.fullId) {
							currentTokenArrayIndex = tokens.size - 1
							currentLink = link
						}

						val newPosTag = token.tag()

						// build link with new pos tag
						val newLink = link.copy(posTags = link.posTags :+ newPosTag)
						// store it back in position info, so we accumulate all tags and ..
						positionInfo.put(range, newLink)
						// .. store it in the output
						arrayOffsetToLink(currentTokenArrayIndex) = newLink
					case None =>
				}
			}
		}
		(tokens.toArray, arrayOffsetToLink)
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
