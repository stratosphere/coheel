package de.uni_potsdam.hpi.coheel.programs

import eu.stratosphere.api.scala.operators.{CsvInputFormat, CsvOutputFormat}
import de.uni_potsdam.hpi.coheel.Main

object OutputFiles {
	lazy val currentPath = Main.config.getString("output_files_dir")

	lazy val surfaceProbsPath           = s"file://$currentPath/testoutput/surface-probs.wiki"
	lazy val contextLinkProbsPath       = s"file://$currentPath/testoutput/context-link-probs-test.wiki"
	lazy val languageModelsPath         = s"file://$currentPath/testoutput/language-models.wiki"
	lazy val documentFrequencyPath      = s"file://$currentPath/testoutput/document-frequency-counts.wiki"
	lazy val redirectPath               = s"file://$currentPath/testoutput/redirects-test.wiki"
	lazy val surfaceDocumentPath        = s"file://$currentPath/testoutput/surface-document-counts.wiki"
	lazy val textDumpsPath              = s"file://$currentPath/testoutput/text-dumps.wiki"
	lazy val surfaceOccurrenceCountPath = s"file://$currentPath/testoutput/surface-occurence-counts.wiki"
	lazy val redirectResolvPath         = s"file://$currentPath/testoutput/resolved-redirects.wiki"

	val textFormat            = CsvOutputFormat[(String, String)]("\n", "\t")
	val textInput             = CsvInputFormat[(String, String)]("\n", '\t')

	val surfaceDocumentFormat = CsvOutputFormat[(String, Int)]("\n", "\t")
	val surfaceDocumentInput  = CsvInputFormat[(String, Int)]("\n", '\t')

	val probOutputFormat      = CsvOutputFormat[(String, String, Double)]("\n", "\t")
	val probInputFormat       = CsvInputFormat[(String, String, Double)]("\n", '\t')

	val linkOccurrenceFormat  = CsvOutputFormat[(String, Int, Int)]("\n", "\t")
}
