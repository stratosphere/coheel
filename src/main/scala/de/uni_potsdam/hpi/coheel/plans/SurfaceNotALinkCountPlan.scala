package de.uni_potsdam.hpi.coheel.plans

import eu.stratosphere.api.common.{Program, ProgramDescription, Plan}
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import OutputFiles._
import de.uni_potsdam.hpi.coheel.wiki.TextAnalyzer

class SurfaceNotALinkCountPlan extends Program with ProgramDescription {

	override def getDescription = "Training the model parameters for CohEEL."

	override def getPlan(args: String*): Plan = {

		val languageModels = DataSource(languageModelsPath, probInputFormat).map { case (doc, word, _) =>
			(doc, word)
		}
		val surfaces = TextFile(surfaceProbsPath).map { line =>
			val split = line.split('\t')
			(split(0), split(1), split(2).toDouble)
		}.map { case (text, _, _) =>
			val tokens = TextAnalyzer.tokenize(text)
				if (tokens.isEmpty)
					// TODO: Print these, and look, why they are not tokenizable
					text
				else
					tokens.head
		}



		val documentOccurrences = languageModels.cogroup(surfaces)
			.where { case (document, word) => word }
			.isEqualTo { word => word }
			.map { case (lmIt, surfaceIt) =>
				lmIt.map { case (doc, _) => doc }
			}

		val output = documentOccurrences.map { l =>
			l.size
		}

		// TODO:
		// Join surfaces with language models
		// Find out documents, where the words occur
		// Intersection of those documents
		// Check whether the mention really occurs in the text


		val surfaceLinkOccurrenceOutput = output.write(surfaceOccurrenceCountPath,
			CsvOutputFormat[Int]("\n", "\t"))
		val plan = new ScalaPlan(Seq(surfaceLinkOccurrenceOutput))
		plan
	}
}
