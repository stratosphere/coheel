package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.FlinkProgramRunner
import org.apache.flink.api.scala.operators.{CsvInputFormat, CsvOutputFormat}

object OutputFiles {
	lazy val currentPath = FlinkProgramRunner.config.getString("output_files_dir")

	lazy val surfaceProbsPath          = s"file://$currentPath/testoutput/surface-probs.wiki"
	lazy val contextLinkProbsPath      = s"file://$currentPath/testoutput/context-link-probs.wiki"
	lazy val languageModelProbsPath    = s"file://$currentPath/testoutput/language-model-probs.wiki"
	lazy val documentFreqsPath         = s"file://$currentPath/testoutput/document-frequency-freqs.wiki"
	lazy val redirectPath              = s"file://$currentPath/testoutput/redirects.wiki"
	lazy val resolvedRedirectsPath     = s"file://$currentPath/testoutput/resolved-redirects.wiki"
	lazy val textDumpsPath             = s"file://$currentPath/testoutput/text-dumps.wiki"
	lazy val surfaceDocumentFreqsPath  = s"file://$currentPath/testoutput/surface-document-freqs.wiki"
	lazy val surfaceNotALinkFreqsPath  = s"file://$currentPath/testoutput/surface-not-a-link-freqs.wiki"


	val textFormat            = CsvOutputFormat[(String, String)]("\n", "\t")
	val textInput             = CsvInputFormat[(String, String)]("\n", '\t')

	val surfaceDocumentFormat = CsvOutputFormat[(String, Int)]("\n", "\t")
	val surfaceDocumentInput  = CsvInputFormat[(String, Int)]("\n", '\t')

	val probOutputFormat      = CsvOutputFormat[(String, String, Double)]("\n", "\t")
	val probInputFormat       = CsvInputFormat[(String, String, Double)]("\n", '\t')

	val linkOccurrenceFormat  = CsvOutputFormat[(String, Int, Int)]("\n", "\t")
}
