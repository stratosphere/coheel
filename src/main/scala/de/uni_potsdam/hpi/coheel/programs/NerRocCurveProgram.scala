package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.PerformanceTimer
import de.uni_potsdam.hpi.coheel.datastructures.{TrieBuilder, Trie}
import de.uni_potsdam.hpi.coheel.wiki.{WikiPage, TokenizerHelper}
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper.Token
import org.apache.flink.api.common.ProgramDescription
import org.apache.flink.api.scala._
import org.apache.log4j.Logger
import OutputFiles._

class NerRocCurveProgram extends CoheelProgram with ProgramDescription {

	@transient val log: Logger = Logger.getLogger(this.getClass)

	TrieBuilder.buildFullTrie()

	override def getDescription = "Determining the ROC curve for the NER threshold."

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val wikiPages = ProgramHelper.filterNormalPages(ProgramHelper.getWikiPages(env))

		val rocValues = wikiPages.mapPartition { partitionIt =>
			val partition = partitionIt.toList
			println(s"Size of this partition: ${partition.size}")
			if (partition.isEmpty) {
				List()
			}
			else {
				(0.1 to 0.9 by 0.1).map { threshold =>
					println(s"MAX  : ${Runtime.getRuntime.maxMemory()}")
					println(s"FREE : ${Runtime.getRuntime.freeMemory()}")
					println(s"TOTAL: ${Runtime.getRuntime.totalMemory()}")
					println(f"Working on threshold $threshold%.2f.")
					TrieBuilder.buildThresholdTrie(threshold)
					val thresholdTrie: Trie = TrieBuilder.thresholdTrie

					var tp = 0
					var tn = 0
					var fp = 0
					var fn = 0
					var i = 0
					partition.foreach { wikiPage =>
//						if (i % 100 == 0)
//							println(i)
						i += 1
						// determine the actual surfaces, from the real wikipedia article
						val actualSurfaces = wikiPage.links.map { link => link.surface}.toSet
//						log.warn(wikiPage.pageTitle)

						// determine potential surfaces, i.e. the surfaces that the NER would return for the current
						// threshold
						val potentialSurfaces = NerRocCurveProgram.determinePotentialSurfaces(wikiPage, thresholdTrie)
						// determine the potential surfaces for a trie with all surfaces, i.e. the threshold is
						// zero
						val fullPotentialSurfaces = NerRocCurveProgram.determinePotentialSurfaces(wikiPage, TrieBuilder.fullTrie)
						// TPs are those surfaces, which are actually in the text and our system would return it (for the
						// current trie/threshold)
						tp += actualSurfaces.intersect(potentialSurfaces).size
						// TNs are those surfaces, which would be returned for threshold zero, but aren't because of the
						// current threshold, and are no actual surfaces.
						tn += fullPotentialSurfaces.diff(potentialSurfaces).diff(actualSurfaces).size
						// FPs are those surfaces, which are returned but are not actually surfaces
						fp += potentialSurfaces.diff(actualSurfaces).size
						// FN are those surfaces, which are actual surfaces, but are not returned
						fn += actualSurfaces.diff(potentialSurfaces).size
					}
					// output threshold as a string, because otherwise rounding errors occur
					(f"$threshold%.2f", tp, tn, fp, fn, tp.toDouble / (tp + fn), fp.toDouble / (fp + tn))
				}
			}
		}

		rocValues.writeAsTsv(nerRocCurvePath)
	}

}

object NerRocCurveProgram {

	def determinePotentialSurfaces(wikiPage: WikiPage, trie: Trie): Set[String] =  {
		var potentialSurfaces = Set[String]()

		val tokens = TokenizerHelper.tokenizeWithPositions(wikiPage.plainText).toArray

		for (i <- 0 until tokens.size) {
			val resultTokens = trie.slidingContains[Token](tokens, { token: Token => token.word }, i)
			potentialSurfaces ++= resultTokens.map { containment =>
				// extract the real world surfaces, by taking the whole string from the start of the first
				// part to the end of the last part
				wikiPage.plainText.substring(containment.head.startOffset, containment.last.endOffset)
			}
		}
		potentialSurfaces
	}

}
