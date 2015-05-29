package de.uni_potsdam.hpi.coheel.wiki

import de.uni_potsdam.hpi.coheel.programs.DataClasses.Link
import de.uni_potsdam.hpi.coheel.util.Timer
import edu.stanford.nlp.ling.{CoreLabel, HasWord, TaggedWord}
import edu.stanford.nlp.tagger.maxent.MaxentTagger
import org.apache.flink.shaded.com.google.common.collect.TreeRangeMap

import scala.collection.JavaConverters._
import scala.collection.mutable

class KeepLinkTokenizer(positionInfo: TreeRangeMap[Integer, Link], tagger: MaxentTagger) {
	// stores the tokens
	private val tokens = mutable.ArrayBuffer[String]()
	// store the pos tags
	private val tags = mutable.ArrayBuffer[String]()
	// maps array offsets to link positions
	private val linkPositions = mutable.Map[Int, Link]()
	def getTokens: mutable.ArrayBuffer[String] = tokens
	def getLinkPositions: mutable.Map[Int, Link] = linkPositions
	def getTags: mutable.ArrayBuffer[String] = tags


	// the following two variables keep track of the translation from string indices to token array indices
	// currentTokenArrayIndex stores the index for the last link we found while processing the text
	private var currentTokenArrayIndex = 0
	// currentLink stores the last link we saw
	private var currentLink: Link = null

	def processSentence(sent: java.util.List[HasWord]): Unit = {
		// TODO: Only tokenize if necessary.
		val sentenceTags = tagSentence(sent)
		sentenceTags.foreach(processToken)
	}
	private def processToken(token: TaggedWord): Unit = {
		tokens += token.word()
		tags   += token.tag()
		val startOffset = token.beginPosition()
		Option(positionInfo.getEntry(startOffset)).foreach { entry =>
			val range = entry.getKey
			val link = entry.getValue
			// check, whether a new link started, then build a new index, use old link offset otherwise
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
			linkPositions(currentTokenArrayIndex) = newLink
		}
	}

	private def tagSentence(sent: java.util.List[HasWord]): mutable.Buffer[TaggedWord] = {
		Timer.start("TAGGING")
		val ret = tagger.tagSentence(sent).asScala
		Timer.end("TAGGING")
		ret
//		sent.asScala.map { word =>
//			val tw = new TaggedWord(word.word(), "NP")
//			tw.setBeginPosition(word.asInstanceOf[CoreLabel].beginPosition())
//			tw
//		}
	}
}
