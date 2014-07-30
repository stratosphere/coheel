package de.uni_potsdam.hpi.coheel.plans

import eu.stratosphere.api.scala.operators.{CsvInputFormat, CsvOutputFormat}

object OutputFiles {
	lazy val currentPath = System.getProperty("user.dir")

	lazy val surfaceProbsPath           = s"file://$currentPath/testoutput/surface-probs.wiki"
	lazy val contextLinkProbsPath       = s"file://$currentPath/testoutput/context-link-probs.wiki"
	lazy val languageModelsPath         = s"file://$currentPath/testoutput/language-models.wiki"
	lazy val redirectPath               = s"file://$currentPath/testoutput/redirects.wiki"
	lazy val surfaceDocumentPath        = s"file://$currentPath/testoutput/surface-document-counts.wiki"
	lazy val textDumpsPath              = s"file://$currentPath/testoutput/text-dumps.wiki"
	lazy val surfaceOccurrenceCountPath = s"file://$currentPath/testoutput/surface-occurence-counts.wiki"

	val textFormat            = CsvOutputFormat[(String, String)]("\n", "\t")
	val textInput             = CsvInputFormat[(String, String)]("\n", '\t')

	val surfaceDocumentFormat = CsvOutputFormat[(String, Int)]("\n", "\t")
	val surfaceDocumentInput  = CsvInputFormat[(String, Int)]("\n", '\t')

	val probOutputFormat      = CsvOutputFormat[(String, String, Double)]("\n", "\t")
	val probInputFormat       = CsvInputFormat[(String, String, Double)]("\n", '\t')

	val linkOccurrenceFormat  = CsvOutputFormat[(String, Int, Int)]("\n", "\t")
}
