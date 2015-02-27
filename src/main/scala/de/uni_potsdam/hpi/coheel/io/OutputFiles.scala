package de.uni_potsdam.hpi.coheel.io

import de.uni_potsdam.hpi.coheel.FlinkProgramRunner
import org.apache.flink.api.java.operators.DataSink
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import scala.language.implicitConversions

object OutputFiles {
	lazy val outputPath = FlinkProgramRunner.config.getString("output_files_dir")
	lazy val location = FlinkProgramRunner.config.getString("type")

	lazy val surfaceProbsPath            = s"$location://${outputPath}surface-probs.wiki"
	lazy val contextLinkProbsPath        = s"$location://${outputPath}context-link-probs.wiki"
	lazy val languageModelProbsPath      = s"$location://${outputPath}language-model-probs.wiki"
	lazy val documentWordCountsPath      = s"$location://${outputPath}document-word-counts.wiki"
	lazy val redirectPath                = s"$location://${outputPath}redirects.wiki"
	lazy val resolvedRedirectsPath       = s"$location://${outputPath}resolved-redirects.wiki"
	lazy val wikiPagesPath               = s"$location://${outputPath}wiki-pages.wiki"
	lazy val plainTextsPath              = s"$location://${outputPath}plain-texts.wiki"
	lazy val surfaceDocumentCountsPath   = s"$location://${outputPath}surface-document-counts.wiki"
	lazy val entireTextSurfacesPath      = s"$location://${outputPath}entire-text-surfaces.wiki"
	lazy val surfaceLinkProbsPath        = s"$location://${outputPath}surface-link-probs.wiki"

	lazy val surfaceEvaluationPath       = s"$location://${outputPath}surface-evaluation.wiki"
	lazy val surfaceEvaluationPerDocPath = s"$location://${outputPath}surface-evaluation-per-document.wiki"

	lazy val classificationPath          = s"$location://${outputPath}classification.wiki"

	implicit def toOutputFiles(dataSet: DataSet[_]): OutputFiles = {
		new OutputFiles(dataSet)
	}

	val LINE_DELIMITER = "\n"
	val ROW_DELIMITER  = '\t'
}

class OutputFiles(dataSet: DataSet[_]) {

	def writeAsTsv(path: String): DataSink[_] = {
		dataSet.writeAsCsv(path, OutputFiles.LINE_DELIMITER, OutputFiles.ROW_DELIMITER.toString, FileSystem.WriteMode.OVERWRITE)
	}
}
