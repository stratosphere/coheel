package de.uni_potsdam.hpi.coheel.programs

import java.util.Collections

import de.uni_potsdam.hpi.coheel.io.Sample
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper
import org.apache.flink.api.scala._
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import scala.collection.mutable

class ClassificationProgram extends CoheelProgram {

	override def getDescription: String = "CohEEL Classification"

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val documents = env.fromElements(Sample.ANGELA_MERKEL_SAMPLE_TEXT).map { doc =>
			TokenizerHelper.tokenize(doc, stemming = false)
		}
		val surfaceProbs = env.readTextFile(surfaceProbsPath).flatMap { line =>
			val split = line.split('\t')
			if (split.size > 1) {
				val tokens = TokenizerHelper.tokenize(split(0), stemming = false)
				if (tokens.nonEmpty)
					Some(SurfaceProbLink(tokens, split(1), split(2).toDouble))
				else
					None
			}
			else None
		}

		val result = documents.crossWithHuge(surfaceProbs).flatMap { value =>
			val (text, link) = value
			val surface = link.surface
			if (text.containsSlice(surface))
				List((surface.mkString(" "), link.destination, link.prob))
			else
				List()
		}

		result.writeAsTsv(classificationPath)
	}

}
