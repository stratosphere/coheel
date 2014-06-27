package de.uni_potsdam.hpi.coheel.plans

import eu.stratosphere.api.common.{Program, ProgramDescription, Plan}
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import OutputFiles._
import de.uni_potsdam.hpi.coheel.wiki.TextAnalyzer

class SurfaceNotALinkCountPlan extends Program with ProgramDescription {

	override def getDescription = "Training the model parameters for CohEEL."


	override def getPlan(args: String*): Plan = {
		val textAnalyzer = new TextAnalyzer

		val surfaces       = TextFile(surfaceDocumentPath)
		val languageModels = TextFile(languageModelsPath)

		val linkOccurrenceCounts = surfaces.map { s =>
			("mention", 4, 10)
		}

		// TODO:
		// Join surfaces with language models
		// Find out documents, where the words occur
		// Intersection of those documents
		// Check whether the mention really occurs in the text
		//      --> TODO: ask Toni, how to do this (reread original file? save positions?)


		val surfaceLinkOccurrenceOutput = linkOccurrenceCounts.write(linkOccurenceCounts, linkOccurrenceFormat)
		val plan = new ScalaPlan(Seq(surfaceLinkOccurrenceOutput))
		plan
	}
}
