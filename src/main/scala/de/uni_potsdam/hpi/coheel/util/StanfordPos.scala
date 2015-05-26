package de.uni_potsdam.hpi.coheel.util

import java.io.StringReader

import edu.stanford.nlp.ling
import edu.stanford.nlp.ling.{CoreLabel, Word, TaggedWord, HasWord}
import edu.stanford.nlp.process.DocumentPreprocessor
import edu.stanford.nlp.process.PTBTokenizer.PTBTokenizerFactory
import edu.stanford.nlp.tagger.maxent.MaxentTagger

import scala.collection.JavaConverters._
import scala.collection.mutable

object StanfordPos {
	val modelName = "edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger"
	val tagger = new MaxentTagger(modelName)

	def tagSentence(s: java.util.List[HasWord]): mutable.Buffer[TaggedWord] = {
//		tagger.tagSentence(s).asScala
		s.asScala.map { word =>
			val tw = new ling.TaggedWord(word.word(), "NP")
			tw.setBeginPosition(word.asInstanceOf[CoreLabel].beginPosition())
			tw
		}
	}
	def tagPOS(s: String): mutable.Map[Int, String] = {
		val sentences = MaxentTagger.tokenizeText(new StringReader(s)).asScala
		val tags = mutable.Map[Int, String]()

		sentences.foreach { sentence =>
			val sentenceTags = tagger.tagSentence(sentence)
			sentenceTags.asScala.foreach { tag =>
				tags(tag.beginPosition()) = tag.tag()
			}
		}
		tags
	}

	def main(args: Array[String]) {
		val text = "Pierre Vinken (61 years old) will join the board as a nonexecutive director Nov. 29.\n" +
			"Mr. Vinken is chairman of Elsevier N.V., the Dutch publishing group.\n" +
			"Rudolph Agnew, 55 years old and former chairman of Consolidated Gold Fields PLC, was named a director of this British industrial conglomerate.\n"
		println(tagger.getTags)
		println("======================")
		for (i <- 0 until tagger.numTags())
			println(tagger.getTag(i))
		println("======================")
		val textReader = new StringReader(text)

		val prep = new DocumentPreprocessor(textReader)
		val tokenizer = PTBTokenizerFactory.newCoreLabelTokenizerFactory("normalizeParentheses=false,normalizeOtherBrackets=false,untokenizable=noneKeep")
		prep.setTokenizerFactory(tokenizer)
		val sentences = prep.iterator().asScala
//		val sentences = tokenizer.getTokenizer(textReader).

		sentences.foreach { sentence =>
			println("sentence: " + sentence)
			val words = tagger.tagSentence(sentence)
			words.asScala.foreach { w =>
				println(s"${w.beginPosition}-${w.endPosition}\t${w.word}: <${w.tag}>")
			}
			println("----------------------")
		}
	}
}

