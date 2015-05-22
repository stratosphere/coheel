package de.uni_potsdam.hpi.coheel.util

import java.io.StringReader
import edu.stanford.nlp.tagger.maxent.MaxentTagger
import scala.collection.JavaConverters._

object StanfordPosMain {
	def main(args: Array[String]) {
		val text = "Pierre Vinken, 61 years old, will join the board as a nonexecutive director Nov. 29.\n" +
			"Mr. Vinken is chairman of Elsevier N.V., the Dutch publishing group.\n" +
			"Rudolph Agnew, 55 years old and former chairman of Consolidated Gold Fields PLC, was named a director of this British industrial conglomerate.\n"
		val modelName = "edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger"
		val tagger = new MaxentTagger(modelName)
		println(tagger.getTags)
		println("======================")
		for (i <- 0 until tagger.numTags())
			println(tagger.getTag(i))
		println("======================")
		val textReader = new StringReader(text)
		val sentences = MaxentTagger.tokenizeText(textReader)
		sentences.asScala.foreach { sentence =>
			println("sentence: " + sentence)
			val words = tagger.tagSentence(sentence)
			words.asScala.foreach { w =>
				println(s"${w.beginPosition}-${w.endPosition}\t${w.word}: <${w.tag}>")
			}
			println("----------------------")
		}
	}
}

