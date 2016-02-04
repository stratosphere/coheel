package de.uni_potsdam.hpi.coheel.wiki

import de.uni_potsdam.hpi.coheel.programs.DataClasses.Link
import edu.stanford.nlp.ling.TaggedWord
import edu.stanford.nlp.tagger.maxent.MaxentTagger
import org.apache.flink.shaded.com.google.common.collect.TreeRangeMap

import scala.collection.mutable

class TextIndexToTokenIndexTranslator(positionInfo: TreeRangeMap[Integer, Link]) {
	if (positionInfo == null)
		throw new IllegalArgumentException("positionInfo must not be nil")

	// maps array offsets to link positions
	val linkPositions = mutable.Map[Int, Link]()


	// the following two variables keep track of the translation from string indices to token array indices
	// currentTokenArrayIndex stores the index for the last link we found while processing the text
	private var currentTokenArrayIndex = 0
	// currentLink stores the last link we saw
	private var currentLink: Link = null

	def translateLinks(sent: mutable.Buffer[TaggedWord]): mutable.Buffer[TaggedWord] = {
		sent.foreach(processToken)
		sent
	}

	var tokenNr = 0
	private def processToken(token: TaggedWord): Unit = {
		tokenNr += 1
		val startOffset = token.beginPosition()

		Option(positionInfo.getEntry(startOffset)).foreach { entry =>
			val range = entry.getKey
			val link = entry.getValue
			// check, whether a new link started, then build a new index, use old link offset otherwise
			if (currentLink == null || currentLink.id != link.id) {
				currentTokenArrayIndex = tokenNr - 1
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
}
