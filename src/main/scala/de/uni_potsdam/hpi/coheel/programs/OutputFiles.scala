package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.FlinkProgramRunner
import org.apache.flink.api.java.operators.DataSink
import org.apache.flink.api.scala.DataSet
import org.apache.flink.core.fs.FileSystem

object OutputFiles {
	lazy val currentPath = FlinkProgramRunner.config.getString("output_files_dir")

	lazy val surfaceProbsPath            = s"file://${currentPath}surface-probs.wiki"
	lazy val contextLinkProbsPath        = s"file://${currentPath}context-link-probs.wiki"
	lazy val languageModelProbsPath      = s"file://${currentPath}language-model-probs.wiki"
	lazy val documentWordCountsPath      = s"file://${currentPath}document-word-counts.wiki"
	lazy val redirectPath                = s"file://${currentPath}redirects.wiki"
	lazy val resolvedRedirectsPath       = s"file://${currentPath}resolved-redirects.wiki"
	lazy val textDumpsPath               = s"file://${currentPath}text-dumps.wiki"
	lazy val surfaceDocumentCountsPath   = s"file://${currentPath}surface-document-counts.wiki"
	lazy val entireTextSurfacesPath      = s"file://${currentPath}entire-text-surfaces.wiki"
	lazy val surfaceLinkProbsPath        = s"file://${currentPath}surface-link-probs.wiki"
	lazy val nerRocCurvePath             = s"file://${currentPath}ner-roc-curve.wiki"

	implicit def toOutputFiles(dataSet: DataSet[_]): OutputFiles = {
		new OutputFiles(dataSet)
	}
}

class OutputFiles(dataSet: DataSet[_]) {

	def writeAsTsv(path: String): DataSink[_] = {
		dataSet.writeAsCsv(path, "\n", "\t", FileSystem.WriteMode.OVERWRITE)
	}

}
