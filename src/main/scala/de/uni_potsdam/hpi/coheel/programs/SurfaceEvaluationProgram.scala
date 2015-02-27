package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.Timer
import de.uni_potsdam.hpi.coheel.datastructures.NewTrie
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.programs.DataClasses.Plaintext
import de.uni_potsdam.hpi.coheel.wiki.{TokenizerHelper, WikiPage}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import scala.collection.mutable


object SurfaceEvaluationProgram {
	val BROADCAST_SURFACES = "surfaces"
}
class SurfaceEvaluationProgram extends CoheelProgram[Int] {

	override def getDescription = "Surface Evaluation"

	val params = if (runsOffline()) List(-1) else 1 to 1

	override def buildProgram(env: ExecutionEnvironment, param: Int): Unit = {
		val currentFile = if (runsOffline()) "" else s"/$param"
		val surfaceLinkProbs = getSurfaceLinkProbs(currentFile)
		val plainTexts = getPlainTexts()

		val surfaceEvaluation = plainTexts
			.flatMap(new SurfaceEvaluationFlatMap)
			.withBroadcastSet(surfaceLinkProbs, EntireTextSurfacesProgram.BROADCAST_SURFACES)
		.name("Entire-Text-Surfaces-Along-With-Document")

		surfaceEvaluation.writeAsTsv(surfaceEvaluationPath)
	}
}

case class Evaluation(threshold: String, actualSurfaces: Int, potentialSurfaces: Int, tp: Int, fp: Int, subsetFp: Int, fn: Int) {
	override def toString: String = {
		s"Evaluation(threshold=$threshold,actualSurfaces=$actualSurfaces,potentialSurfaces=$potentialSurfaces,tp=$tp,fp=$fp,subsetFp=$subsetFp,fn=$fn)"
	}
}

class SurfaceEvaluationFlatMap extends RichFlatMapFunction[Plaintext, (String, Evaluation)] {

	var trie: NewTrie = _

	override def open(params: Configuration): Unit = {
		trie = getRuntimeContext.getBroadcastVariableWithInitializer(EntireTextSurfacesProgram.BROADCAST_SURFACES, new TrieWithProbBroadcastInitializer)
	}
	override def flatMap(plainText: Plaintext, out: Collector[(String, Evaluation)]): Unit = {
		// determine the actual surfaces, from the real wikipedia article
		val actualSurfaces = plainText.linkString.split(CoheelProgram.LINK_SPLITTER).map(_.split(" ").toSeq).toSet

		// determine potential surfaces, i.e. the surfaces that the NER would return
		Timer.start("FINDALL IN TRIE")
		var potentialSurfacesWithProbs = trie.findAllInWithProbs(plainText.plainText)
			.map { case (surface, prob) => (surface.split(' ').toSeq, prob) }
			.to[mutable.MutableList]
		Timer.end("FINDALL IN TRIE")

		if (plainText.pageTitle == "My test article") {
			println(potentialSurfacesWithProbs.toList)
		}

		(0.0f to 0.95f by 0.05f).foreach { threshold =>
			Timer.start("FILTER DOWN")
			potentialSurfacesWithProbs = potentialSurfacesWithProbs.filter(_._2 >= threshold)
			Timer.end("FILTER DOWN")
			Timer.start("BUILD SET")
			val potentialSurfaces = potentialSurfacesWithProbs.map(_._1).toSet
			Timer.end("BUILD SET")

			Timer.start("TP")
			// TPs are those surfaces, which are actually in the text and our system would return it
			val tp = actualSurfaces.intersect(potentialSurfaces)
			Timer.end("TP")
			// FPs are those surfaces, which are returned but are not actually surfaces
			Timer.start("FP")
			val fp = potentialSurfaces.diff(actualSurfaces)
			Timer.end("FP")

			Timer.start("SUBSET FP")
			val subsetFp = fp.count { fpSurface =>
				tp.find { tpSurface =>
					tpSurface.containsSlice(fpSurface)
				} match {
					case Some(superSurface) =>
//						println(s"'${wikiPage.pageTitle}': Found $superSurface for $fpSurface.")
						true
					case None =>
						false
				}
			}
			Timer.end("SUBSET FP")

			Timer.start("FN")
			// FN are those surfaces, which are actual surfaces, but are not returned
			val fn = actualSurfaces.diff(potentialSurfaces)
			Timer.end("FN")
			out.collect(plainText.pageTitle, Evaluation(f"$threshold%.2f", actualSurfaces.size, potentialSurfaces.size, tp.size, fp.size, subsetFp, fn.size))
		}
	}

	override def close(): Unit = {
		Timer.printAll()
	}
}
