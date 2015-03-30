package de.uni_potsdam.hpi.coheel.io

import de.uni_potsdam.hpi.coheel.FlinkProgramRunner
import de.uni_potsdam.hpi.coheel.programs.DataClasses.LanguageModel
import org.apache.flink.api.common.io.FileOutputFormat
import org.apache.flink.api.common.io.FileOutputFormat.OutputDirectoryMode
import org.apache.flink.api.java.operators.DataSink
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.operators.ScalaCsvOutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.{Path, FileSystem}
import scala.language.implicitConversions

object OutputFiles {
	lazy val outputPath = FlinkProgramRunner.config.getString("output_files_dir")
	lazy val location = FlinkProgramRunner.config.getString("type")

	lazy val surfaceProbsPath                 = s"$location://${outputPath}surface-probs.wiki"
	lazy val contextLinkProbsPath             = s"$location://${outputPath}context-link-probs.wiki"
	lazy val languageModelsPath               = s"$location://${outputPath}language-model-probs.wiki"
	lazy val documentWordCountsPath           = s"$location://${outputPath}document-word-counts.wiki"
	lazy val redirectPath                     = s"$location://${outputPath}redirects.wiki"
	lazy val resolvedRedirectsPath            = s"$location://${outputPath}resolved-redirects.wiki"
	lazy val wikiPagesPath                    = s"$location://${outputPath}wiki-pages.wiki"
	lazy val plainTextsPath                   = s"$location://${outputPath}plain-texts.wiki"
	lazy val allLinksPath                     = s"$location://${outputPath}all-links.wiki"
	lazy val surfaceDocumentCountsPath        = s"$location://${outputPath}surface-document-counts.wiki"
	lazy val entireTextSurfacesPath           = s"$location://${outputPath}entire-text-surfaces.wiki"
	lazy val surfaceLinkProbsPath             = s"$location://${outputPath}surface-link-probs.wiki"

	lazy val surfaceEvaluationPerDocumentPath = s"$location://${outputPath}surface-evaluation-per-document.wiki"
	lazy val surfaceEvaluationPerSubsetPath   = s"$location://${outputPath}surface-evaluation-per-subset.wiki"
	lazy val surfaceEvaluationPath            = s"$location://${outputPath}surface-evaluation.wiki"

	lazy val classificationPath               = s"$location://${outputPath}classification.wiki"

	lazy val pageRankPath                     = s"$location://${outputPath}page-rank.wiki"

	implicit def toOutputFiles(dataSet: DataSet[_]): OutputFiles = {
		new OutputFiles(dataSet)
	}

	val RECORD_DELIMITER = "\n"
	val FIELD_DELIMITER  = "\t"
}

class OutputFiles(dataSet: DataSet[_]) {

	def writeAsTsv(path: String): DataSink[_] = {
		dataSet.writeAsCsv(path, OutputFiles.RECORD_DELIMITER, OutputFiles.FIELD_DELIMITER, FileSystem.WriteMode.OVERWRITE)
	}
}


class LanguageModelOutputFormat extends FileOutputFormat[LanguageModel]() {

	val output = new ScalaCsvOutputFormat[(String, String, Double)](outputFilePath,
		OutputFiles.RECORD_DELIMITER, OutputFiles.FIELD_DELIMITER)

	override def getOutputFilePath: Path = output.getOutputFilePath
	override def setOutputDirectoryMode(mode: OutputDirectoryMode): Unit = output.setOutputDirectoryMode(mode)
	override def configure(parameters: Configuration): Unit = output.configure(parameters)
	override def getWriteMode: WriteMode = output.getWriteMode
	override def getOutputDirectoryMode: OutputDirectoryMode = output.getOutputDirectoryMode
	override def setOutputFilePath(outputPath: Path): Unit = output.setOutputFilePath(outputPath)
	override def close(): Unit = output.close()
	override def setWriteMode(mode: WriteMode): Unit = output.setWriteMode(mode)
	override def initializeGlobal(parallelism: Int): Unit = output.initializeGlobal(parallelism)
	override def tryCleanupOnError(): Unit = output.tryCleanupOnError()

	override def open(taskNumber: Int, numTask: Int): Unit = output.open(taskNumber, numTask)

	override def writeRecord(record: LanguageModel): Unit = {
		record.model.foreach { case (word, prob) =>
			output.writeRecord((record.pageTitle, word, prob))
		}
	}
}
