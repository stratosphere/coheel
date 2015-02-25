package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.datastructures.Trie
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.wiki.{WikiPage, TokenizerHelper}
import org.apache.flink.api.scala._
import org.apache.log4j.Logger

class NerRocCurveProgram extends CoheelProgram[Int] {

	@transient val log: Logger = Logger.getLogger(this.getClass)

	override def getDescription = "Determining the ROC curve for the NER threshold."

	override val params: Seq[Int] = List(1)

	override def buildProgram(env: ExecutionEnvironment, param: Int): Unit = {
		val wikiPages = ProgramHelper.filterNormalPages(ProgramHelper.getWikiPages(env))

		val rocValues = wikiPages.mapPartition { partitionIt =>
			val partition = partitionIt.toList
			println(s"Size of this partition: ${partition.size}")
			if (partition.isEmpty) {
				List()
			}
			else {
				var tp = 0
				var tn = 0
				var fp = 0
				var fn = 0
				var i = 0
				partition.foreach { wikiPage =>
					i += 1
					// determine the actual surfaces, from the real wikipedia article
					val actualSurfaces = wikiPage.links.map { link => link.surfaceRepr }.toSet

					// determine potential surfaces, i.e. the surfaces that the NER would return for the current
					// threshold
//					val potentialSurfaces = NerRocCurveProgram.determinePotentialSurfaces(wikiPage, thresholdTrie)

					// TPs are those surfaces, which are actually in the text and our system would return it (for the
					// current trie/threshold)
//					tp += actualSurfaces.intersect(potentialSurfaces).size
					// TNs are those surfaces, which would be returned for threshold zero, but aren't because of the
					// current threshold, and are no actual surfaces.
//					tn += fullPotentialSurfaces.diff(potentialSurfaces).diff(actualSurfaces).size
					// FPs are those surfaces, which are returned but are not actually surfaces
//					fp += potentialSurfaces.diff(actualSurfaces).size
					// FN are those surfaces, which are actual surfaces, but are not returned
//					fn += actualSurfaces.diff(potentialSurfaces).size
				}
				// output threshold as a string, because otherwise rounding errors occur
				List(("", tp, tn, fp, fn, tp.toDouble / (tp + fn), fp.toDouble / (fp + tn)))
			}
		}

		rocValues.writeAsTsv(nerRocCurvePath.replace(".wiki", s"test.wiki"))
	}

}

object NerRocCurveProgram {

	def determinePotentialSurfaces(wikiPage: WikiPage, trie: Trie): Set[String] =  {
		var potentialSurfaces = Set[String]()

//		val tokens = TokenizerHelper.tokenizeWithPositions(wikiPage.plainText).toArray

//		for (i <- 0 until tokens.size) {
//			val resultTokens = trie.slidingContains[Token](tokens, { token: Token => token.word }, i)
//			potentialSurfaces ++= resultTokens.map { containment =>
//				// extract the real world surfaces, by taking the whole string from the start of the first
//				// part to the end of the last part
//				wikiPage.plainText.substring(containment.head.startOffset, containment.last.endOffset)
//			}
//		}
		potentialSurfaces
	}
}
