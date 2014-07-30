package de.uni_potsdam.hpi.coheel.plans

import eu.stratosphere.api.common.{Program, ProgramDescription, Plan}
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import OutputFiles._
import de.uni_potsdam.hpi.coheel.wiki.TextAnalyzer

case class Surface(surfaceText: String, firstWord: String)
case class LanguageModelEntry(doc: String, word: String)

class SurfaceNotALinkCountPlan extends Program with ProgramDescription {

	override def getDescription = "Training the model parameters for CohEEL."

	override def getPlan(args: String*): Plan = {

		val languageModels = DataSource(languageModelsPath, probInputFormat).map { case (doc, word, _) =>
			LanguageModelEntry(doc, word)
		}
		val surfaces = TextFile(surfaceProbsPath).map { line =>
			val split = line.split('\t')
			split(0)
		}.map { surface =>
			val tokens = TextAnalyzer.tokenize(surface)
				if (tokens.isEmpty)
					// TODO: Print these, and look, why they are not tokenizable
					Surface(surface, surface)
				else
					Surface(surface, tokens.head)
		}



		val documentOccurrences = languageModels.join(surfaces)
			.where { case LanguageModelEntry(_, word) => word }
			.isEqualTo { case Surface(_, firstWord) => firstWord }
			.map { case (lmEntry, surface) =>
//				println(surface.surfaceText)
				(surface.surfaceText, lmEntry.doc)
			}.groupBy { case (surfaceText, _) =>
				surfaceText
			}.reduceGroup { it =>
				(it.next._1, 1)
			}

//		val output = documentOccurrences.groupBy { case (_, surface) => surface }
//			.reduceGroup { l =>
//				println(l.next._2)
//				("abc", 1)
//			}

		// TODO:
		// Join surfaces with language models
		// Find out documents, where the words occur
		// Intersection of those documents
		// Check whether the mention really occurs in the text


		val surfaceLinkOccurrenceOutput = documentOccurrences.write(surfaceOccurrenceCountPath,
			CsvOutputFormat[(String, Int)]("\n", "\t"))
		val plan = new ScalaPlan(Seq(surfaceLinkOccurrenceOutput))
		plan
	}
}
