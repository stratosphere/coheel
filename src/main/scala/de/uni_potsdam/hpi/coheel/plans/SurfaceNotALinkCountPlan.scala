package de.uni_potsdam.hpi.coheel.plans

import eu.stratosphere.api.common.{Program, ProgramDescription, Plan}
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import OutputFiles._
import de.uni_potsdam.hpi.coheel.wiki.TextAnalyzer

class SurfaceNotALinkCountPlan extends Program with ProgramDescription {

	override def getDescription = "Training the model parameters for CohEEL."

//	case class LinkCount(linkParts: List[String], )
	override def getPlan(args: String*): Plan = {

		val surfaces       = DataSource(surfaceDocumentPath, surfaceDocumentInput)
		val linkTextAsParts = DataSource(languageModelsPath, inputFormat).map { case (document, linkText, prob) =>
			TextAnalyzer.tokenize(linkText)
		}



//		val documentOccurrenceCounts = languageModels.join(surfaces)
//			.where { case (document, word, prob) => word }
//			.isEqualTo { case (word, prob) => word }
//			.map { case (languageModel, surface) =>
//				// word, document
//				(languageModel._2, languageModel._1)
//			}.groupBy { case (word, document) =>
//				word
//			}.count()

		// TODO:
		// Join surfaces with language models
		// Find out documents, where the words occur
		// Intersection of those documents
		// Check whether the mention really occurs in the text
		//      --> TODO: ask Toni, how to do this (reread original file? save positions?)


//		val surfaceLinkOccurrenceOutput = documentOccurrenceCounts.write(linkOccurenceCounts, linkOccurrenceFormat)
//		val plan = new ScalaPlan(Seq(surfaceLinkOccurrenceOutput))
//		plan
		null
	}
}
