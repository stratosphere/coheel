package de.uni_potsdam.hpi.coheel.programs

import java.util.Date

import de.uni_potsdam.hpi.coheel.datastructures.NewTrie
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.wiki.{TokenizerHelper, WikiPage}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration


object SurfaceEvaluationProgram {
	val BROADCAST_SURFACES = "surfaces"
}
class SurfaceEvaluationProgram extends CoheelProgram[Int] {

	override def getDescription = "Determining the ROC curve for the NER threshold."

	val params = if (runsOffline()) List(-1) else 1 to 10

	override def buildProgram(env: ExecutionEnvironment, param: Int): Unit = {
		val currentFile = if (runsOffline()) "" else s"/$param"
		val surfaces = getSurfaces(currentFile)
		val plainTexts = getWikiPages()

		val rocValues = plainTexts
			.map(new SurfaceEvaluationMap)
			.withBroadcastSet(surfaces, EntireTextSurfacesProgram.BROADCAST_SURFACES)
		.name("Entire-Text-Surfaces-Along-With-Document")

		rocValues.writeAsTsv(surfaceEvaluationPath)
	}
}

class SurfaceEvaluationMap extends RichMapFunction[WikiPage, (String, Int, Int, Int)] {
	var trie: NewTrie = _

	override def open(params: Configuration): Unit = {
		trie = getRuntimeContext.getBroadcastVariableWithInitializer(EntireTextSurfacesProgram.BROADCAST_SURFACES, new TrieBroadcastInitializer)
	}
	override def map(wikiPage: WikiPage): (String, Int, Int, Int) = {
		// determine the actual surfaces, from the real wikipedia article
		val actualSurfaces = wikiPage.links.map { link => link.surfaceRepr}.toSet

		// determine potential surfaces, i.e. the surfaces that the NER would return for the current
		// threshold
		val potentialSurfaces = trie.findAllInWithProbs(TokenizerHelper.transformToTokenized(wikiPage.plainText)).map(_._1).toSet

		// TPs are those surfaces, which are actually in the text and our system would return it (for the
		// current trie/threshold)
		val tp = actualSurfaces.intersect(potentialSurfaces)
		// FPs are those surfaces, which are returned but are not actually surfaces
		val fp = potentialSurfaces.diff(actualSurfaces)
		// FN are those surfaces, which are actual surfaces, but are not returned
		val fn = actualSurfaces.diff(potentialSurfaces)
		(wikiPage.pageTitle, tp.size, fp.size, fn.size)
	}
}
