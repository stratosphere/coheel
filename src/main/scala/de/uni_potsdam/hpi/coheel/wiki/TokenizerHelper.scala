package de.uni_potsdam.hpi.coheel.wiki

import java.io.StringReader

import de.uni_potsdam.hpi.coheel.programs.DataClasses.Link
import de.uni_potsdam.hpi.coheel.util.Timer
import edu.stanford.nlp.ling.{CoreLabel, HasWord, TaggedWord}
import edu.stanford.nlp.process.DocumentPreprocessor
import edu.stanford.nlp.process.PTBTokenizer.PTBTokenizerFactory
import edu.stanford.nlp.tagger.maxent.MaxentTagger
import edu.stanford.nlp.util.logging.RedwoodConfiguration
import org.apache.flink.shaded.com.google.common.collect.TreeRangeMap
import org.tartarus.snowball.ext.PorterStemmer

import scala.collection.JavaConverters._
import scala.collection.mutable
/**
 * Small wrapper around Lucene's tokenizing and stemming.
 */
object TokenizerHelper {

	val TOKENIZER_SETTINGS = "normalizeParentheses=false,normalizeOtherBrackets=false,untokenizable=noneKeep"

	case class TokenizerResult(tokens: mutable.ArrayBuffer[String], tags: mutable.ArrayBuffer[String])
	case class TokenizerResultStemmedAndUnstemmed(tokensStemmed: mutable.ArrayBuffer[String], tokensUnstemmed: mutable.ArrayBuffer[String], tags: mutable.ArrayBuffer[String])
	case class TokenizerResultWithPositionInfo(tokens: mutable.ArrayBuffer[String], tags: mutable.ArrayBuffer[String], translatedLinks: mutable.Map[Int, Link])

	val tagger = {
		// shut off the annoying intialization messages
		RedwoodConfiguration.empty().capture(System.err).apply()
		val modelName = "edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger"
		val t = new MaxentTagger(modelName)
		// enable stderr again
		RedwoodConfiguration.current().clear().apply()
		t
	}

	def tokenize(text: String, stemming: Boolean): Array[String] = {
		val tokens = mutable.ArrayBuffer[String]()
		val rawTokens = tokenStream(text)
		val stemmedOrNot = if (stemming) rawTokens.map(stemSentence) else rawTokens

		stemmedOrNot.foreach { sent =>
			sent.foreach { token =>
				tokens += token.word()
			}
		}
		tokens.toArray
	}

	def tokenizeWithStemmedAndUnstemmed(text: String): TokenizerResultStemmedAndUnstemmed = {
		val tokensUnstemmed = mutable.ArrayBuffer[String]()
		val rawTokens = tokenStream(text).map { sent =>
			sent.foreach { token =>
				tokensUnstemmed += token.word()
			}
			sent
		}

		val tagged = rawTokens.map(tagSentence)
		val stemmed = tagged.map(stemSentence)

		val tokensStemmed = mutable.ArrayBuffer[String]()
		val tags = mutable.ArrayBuffer[String]()
		stemmed.flatMap { it => it }.foreach { taggedWord =>
			tokensStemmed += taggedWord.word()
			tags += taggedWord.tag()
		}

		TokenizerResultStemmedAndUnstemmed(tokensStemmed, tokensUnstemmed, tags)
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
	def tokenizeWithPositionInfo(text: String, positionInfo: TreeRangeMap[Integer, Link], stemming: Boolean): TokenizerResultWithPositionInfo = {
		// method object for translating the link annotations in the full text to link annotations
		// for each token
		val translator = new TextIndexToTokenIndexTranslator(positionInfo)

		val rawTokens = tokenStream(text)
		val tagged = rawTokens.map(tagSentence)
		val stemmedOrNot = if (stemming) tagged.map(stemSentence) else tagged
		val translatedLinks = stemmedOrNot.map(translator.translateLinks)

		val tokens = mutable.ArrayBuffer[String]()
		val tags = mutable.ArrayBuffer[String]()
		translatedLinks.flatMap { it => it }.foreach { taggedWord =>
			tokens += taggedWord.word()
			tags += taggedWord.tag()
		}

		TokenizerResultWithPositionInfo(tokens, tags, translator.linkPositions)
	}

	private def tagSentence(sent: mutable.Buffer[HasWord]): mutable.Buffer[TaggedWord] = {
		Timer.start("TAGGING")
		val ret = tagger.tagSentence(sent.asJava).asScala
		Timer.end("TAGGING")
		ret
	}

	private def stemSentence[T <: HasWord](sent: mutable.Buffer[T]): mutable.Buffer[T] = {
		val stemmer = new PorterStemmer()

		var i = 0
		while (i < sent.size) {
			val token = sent(i)

			stemmer.setCurrent(token.word())
			stemmer.stem()
			val stemmedWord = stemmer.getCurrent.toLowerCase.trim
			token.setWord(stemmedWord)
			i += 1
		}
		sent
	}

	private def tokenStream(text: String): Iterator[mutable.Buffer[HasWord]] = {
		val textReader = new StringReader(text)

		val prep = new DocumentPreprocessor(textReader)
		val tokenizer = PTBTokenizerFactory.newCoreLabelTokenizerFactory(TOKENIZER_SETTINGS)
		prep.setTokenizerFactory(tokenizer)
		val sentences = prep.iterator().asScala

		val PUNCT_REGEX = "\\p{Punct}+".r
		// filter punctuation tokens
		sentences.map { sent =>
			var i = 0
			while (i < sent.size()) {
				val token = sent.get(i)
				// check if current token is only punctuation
				if (PUNCT_REGEX.unapplySeq(token.word()).isEmpty) {
					i += 1
				} else {
					sent.remove(i)
				}
			}
			sent.asScala
		}
	}
}
