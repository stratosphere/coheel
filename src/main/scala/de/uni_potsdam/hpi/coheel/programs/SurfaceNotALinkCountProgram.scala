package de.uni_potsdam.hpi.coheel.programs

import eu.stratosphere.api.common.{Program, ProgramDescription, Plan}
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import OutputFiles._
import de.uni_potsdam.hpi.coheel.wiki.TextAnalyzer

case class Surface(surfaceText: String, firstWord: String)
case class LanguageModelEntry(doc: String, word: String)

class SurfaceNotALinkCountProgram extends Program with ProgramDescription {

	override def getDescription = "Training the model parameters for CohEEL."

	override def getPlan(args: String*): Plan = {

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
					// TODO: Print these, and look, why they are not tokenizable
					Surface(surface, surface)
				else
					Surface(surface, tokens.head)
		}

		val documentOccurrences = languageModels.cogroup(surfaces)
			.where { case LanguageModelEntry(_, word) => word }
			.isEqualTo { case Surface(_, firstWord) => firstWord }
			.flatMap { case (lmEntries, surfaceIt) =>
//				println(surface.surfaceText)
//				(surface.surfaceText, lmEntry.doc)
//				val docs = lmEntries.map { entry => entry.doc }
				val list = lmEntries.toList.map { lmEntry => lmEntry.doc}
				if (list.nonEmpty) {
					surfaceIt.map { surface =>
						(surface.surfaceText, list.size)
					}
//						.flatMap { case (surfaceText, list) =>
//						list.map { doc => (surfaceText, doc)}
//					}
				} else
					List()
			}

		val surfaceLinkOccurrenceOutput = documentOccurrences.write(surfaceOccurrenceCountPath,
			CsvOutputFormat[(String, Int)]("\n", "\t"))
		val plan = new ScalaPlan(Seq(surfaceLinkOccurrenceOutput))
		plan
	}
}
