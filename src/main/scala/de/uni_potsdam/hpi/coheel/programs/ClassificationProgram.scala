package de.uni_potsdam.hpi.coheel.programs

import java.util.Collections

import de.uni_potsdam.hpi.coheel.io.Sample
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper
import org.apache.flink.api.scala._
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import scala.collection.mutable

class ClassificationProgram extends NoParamCoheelProgram {

	override def getDescription: String = "CohEEL Classification"

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val documents = env.fromElements(Sample.ANGELA_MERKEL_SAMPLE_TEXT_1, Sample.ANGELA_MERKEL_SAMPLE_TEXT_2).map { doc =>
			TokenizerHelper.tokenize(doc)
		}

		val surfaces = readSurfaceProbs(0.8)

		val result = documents.crossWithHuge(surfaces).flatMap { value =>
			val (text, surfaceProb) = value
			val surface = surfaceProb.surface
			if (text.containsSlice(surface))
				List((surface.mkString(" "), surfaceProb.destination, surfaceProb.prob))
			else
				List()
		}

		result.writeAsTsv(pageRankPath)
	}

}
