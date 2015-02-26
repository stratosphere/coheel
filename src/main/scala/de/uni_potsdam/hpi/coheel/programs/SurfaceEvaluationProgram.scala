package de.uni_potsdam.hpi.coheel.programs

import java.util.Date

import de.uni_potsdam.hpi.coheel.datastructures.NewTrie
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.wiki.{TokenizerHelper, WikiPage}
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector


object SurfaceEvaluationProgram {
	val BROADCAST_SURFACES = "surfaces"
}
class SurfaceEvaluationProgram extends CoheelProgram[Int] {

	override def getDescription = "Surface Evaluation"

	val params = if (runsOffline()) List(-1) else 1 to 10

	override def buildProgram(env: ExecutionEnvironment, param: Int): Unit = {
		val currentFile = if (runsOffline()) "" else s"/$param"
		val surfaces = getSurfaces(currentFile)
		val plainTexts = getWikiPages()

		val rocValues = plainTexts
			.flatMap(new SurfaceEvaluationFlatMap)
			.withBroadcastSet(surfaces, EntireTextSurfacesProgram.BROADCAST_SURFACES)
		.name("Entire-Text-Surfaces-Along-With-Document")

		rocValues.writeAsTsv(surfaceEvaluationPath)
	}
}

case class Evaluation(threshold: Float, actualSurfaces: Int, potentialSurfaces: Int, tp: Int, fp: Int, subsetFp: Int, fn: Int)

class SurfaceEvaluationFlatMap extends RichFlatMapFunction[WikiPage, (String, Evaluation)] {

	var trie: NewTrie = _

	override def open(params: Configuration): Unit = {
		trie = getRuntimeContext.getBroadcastVariableWithInitializer(EntireTextSurfacesProgram.BROADCAST_SURFACES, new TrieBroadcastInitializer)
	}
	override def flatMap(wikiPage: WikiPage, out: Collector[(String, Evaluation)]): Unit = {
		// determine the actual surfaces, from the real wikipedia article
		val actualSurfaces = wikiPage.links.map { link =>
			link.surfaceRepr.split(' ').toSeq
		}.toSet

		// determine potential surfaces, i.e. the surfaces that the NER would return
		var potentialSurfacesWithProbs = trie.findAllInWithProbs(TokenizerHelper.transformToTokenized(wikiPage.plainText))

		Seq(0.0).foreach { threshold =>
			potentialSurfacesWithProbs = potentialSurfacesWithProbs.filter(_._2 >= threshold || true)
			val potentialSurfaces = potentialSurfacesWithProbs.map { surface =>
				surface._1.split(' ').toSeq
			}.toSet
			// TPs are those surfaces, which are actually in the text and our system would return it
			val tp = actualSurfaces.intersect(potentialSurfaces)
			// FPs are those surfaces, which are returned but are not actually surfaces
			val fp = potentialSurfaces.diff(actualSurfaces)

			val subsetFp = fp.filter { fpSurface =>
				tp.find { tpSurface =>
					tpSurface.containsSlice(fpSurface)
				} match {
					case Some(superSurface) =>
						//					println(s"'${wikiPage.pageTitle}': Found $superSurface for $fpSurface.")
						true
					case None =>
						false
				}
			}

			// FN are those surfaces, which are actual surfaces, but are not returned
			val fn = actualSurfaces.diff(potentialSurfaces)
			if (fn.size >= 1) {
				throw new Exception(s"${wikiPage.pageTitle} has false negatives: $fn.")
			}
			out.collect(wikiPage.pageTitle, Evaluation(0.0f, actualSurfaces.size, potentialSurfaces.size, tp.size, fp.size, subsetFp.size, fn.size))
		}
	}
}
