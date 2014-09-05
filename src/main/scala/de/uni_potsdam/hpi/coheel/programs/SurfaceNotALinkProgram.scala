package de.uni_potsdam.hpi.coheel.programs

import eu.stratosphere.api.common.{Program, ProgramDescription, Plan}
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import OutputFiles._
import de.uni_potsdam.hpi.coheel.wiki.{Extractor, SwebleUtils, TextAnalyzer}
import java.io.{FileReader, BufferedReader}
import org.apache.commons.lang3.StringUtils

case class Surface(surfaceText: String, firstWord: String)
case class LanguageModelEntry(doc: String, word: String)

class SurfaceNotALinkProgram extends Program with ProgramDescription {

	val DOC_NUMBER = 6295

	override def getDescription = "Counting how often a surface occurs, but not as a link."

	override def getPlan(args: String*): Plan = {

		val actualSurfaceOccurrences = TextFile(actualSurfaceOccurrencesPath).map { line =>
			val split = line.split('\t')
			(split(0), split(1).toInt)
		}

		val languageModels = DataSource(languageModelsPath, probInputFormat).map { case (doc, word, _) =>
			LanguageModelEntry(doc, word)
		}
		val surfaces = TextFile(surfaceProbsPath).map { line =>
			val split = line.split('\t')
			split(0)
		}
		// NOTE: This could be done more performant I guess, if we do not group here, but rather
		// preprocess the data to have each surface occur only once
		.groupBy { surface => surface }
		.reduceGroup { surfaceIt => surfaceIt.next() }
		.map { surface =>
			val tokens = TextAnalyzer.tokenize(surface)
				if (tokens.isEmpty)
					// We do not want to link these anyways, because they are no entities
					Surface(surface, surface)
				else
					Surface(surface, tokens.head)
		}

		val possibleOccurrences = languageModels.cogroup(surfaces)
			.where { case LanguageModelEntry(_, word) => word }
			.isEqualTo { case Surface(_, firstWord) => firstWord }
			.flatMap { case (lmEntries, surfaceIt) =>
				val docs = lmEntries.toList.map { lmEntry => lmEntry.doc}
				if (docs.nonEmpty) {
					surfaceIt.map { surface =>
						(surface.surfaceText, surface.firstWord, docs.size)
					}
				} else
					List()
			}.join(actualSurfaceOccurrences)
			.where { case (surfaceText, _, _) => surfaceText }
			.isEqualTo { case (surfaceText, _) => surfaceText }
			.map { case ((surfaceText, firstWord, possibleOccurrences), (_, actualOccurrences)) =>
			(surfaceText, firstWord, actualOccurrences, possibleOccurrences)
		}
		val thresholdEvaluation = possibleOccurrences.flatMap { case (_, _, _, count) =>
				(0.5 to 100.0 by 0.5).map { thresholdPercent =>
					val threshold = thresholdPercent * DOC_NUMBER.toDouble / 100.0
					val missedMentions = if (count > threshold) count else 0
					(thresholdPercent, missedMentions)
				}
			}.groupBy { case (threshold, _) =>
				threshold
			}.reduce { case ((t1, c1), (t2, c2)) => (t1, c1 + c2) }


		val possibleOccurrencesOutput = possibleOccurrences.write(possibleSurfaceOccurrencesPath,
			CsvOutputFormat[(String, String, Int, Int)]("\n", "\t"))
		val actualOccurrencesOutput = actualSurfaceOccurrences.write(actualSurfaceOccurrencesPath,
			CsvOutputFormat[(String, Int)]("\n", "\t"))
		val thresholdEvaluationOutput = thresholdEvaluation.write(thresholdEvaluationPath,
			CsvOutputFormat[(Double, Int)]("\n", "\t"))
		val plan = new ScalaPlan(Seq(possibleOccurrencesOutput))
		plan
	}
}
