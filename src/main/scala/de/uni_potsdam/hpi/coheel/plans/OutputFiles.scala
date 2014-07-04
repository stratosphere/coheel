package de.uni_potsdam.hpi.coheel.plans

import eu.stratosphere.api.scala.operators.{CsvInputFormat, CsvOutputFormat}

object OutputFiles {
	lazy val currentPath = System.getProperty("user.dir")

	lazy val surfaceProbsPath      = s"file://$currentPath/testoutput/surface-probs"
	lazy val contextLinkProbsPath  = s"file://$currentPath/testoutput/context-link-probs"
	lazy val languageModelsPath    = s"file://$currentPath/testoutput/language-models"
	lazy val redirectPath          = s"file://$currentPath/testoutput/redirects"
	lazy val surfaceDocumentPath   = s"file://$currentPath/testoutput/surface-document-counts"
	lazy val linkOccurenceCounts   = s"file://$currentPath/testoutput/link-occurrence-counts"

	val outputFormat          = CsvOutputFormat[(String, String, Int)]("\n", "\t")
	val inputFormat           = CsvInputFormat[(String, String, Int)]("\n", '\t')

	val redirectFormat        = CsvOutputFormat[(String, String)]("\n", "\t")

	val surfaceDocumentFormat = CsvOutputFormat[(String, Int)]("\n", "\t")
	val surfaceDocumentInput  = CsvInputFormat[(String, Int)]("\n", '\t')

	val probOutputFormat      = CsvOutputFormat[(String, String, Double)]("\n", "\t")
	val linkOccurrenceFormat  = CsvOutputFormat[(String, Int, Int)]("\n", "\t")
}
