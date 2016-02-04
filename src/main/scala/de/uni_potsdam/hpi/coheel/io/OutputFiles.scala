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
import scala.util.Try

object OutputFiles {
	lazy val outputPath = FlinkProgramRunner.config.getString("output_files_dir")

	lazy val surfaceProbsPath                 = s"${outputPath}surface-probs.wiki"
	lazy val contextLinkProbsPath             = s"${outputPath}context-link-probs.wiki"
	lazy val languageModelsPath               = s"${outputPath}language-model-probs.wiki"
	lazy val documentWordCountsPath           = s"${outputPath}document-word-counts.wiki"
	lazy val redirectPath                     = s"${outputPath}redirects.wiki"
	lazy val wikiPagesPath                    = s"${outputPath}wiki-pages.wiki"
	lazy val plainTextsPath                   = s"${outputPath}plain-texts.wiki"
	lazy val allLinksPath                     = s"${outputPath}all-links.wiki"
	lazy val surfaceDocumentCountsPath        = s"${outputPath}surface-document-counts.wiki"
	lazy val surfaceDocumentCountsHalfsPath   = s"${outputPath}surface-document-counts-halfs.wiki"
	lazy val surfaceCountHistogramPath        = s"${outputPath}surface-count-histogram.wiki"
	lazy val entireTextSurfacesPath           = s"${outputPath}entire-text-surfaces.wiki"
	lazy val surfaceLinkProbsPath             = s"${outputPath}surface-link-probs.wiki"
	lazy val trainingDataClassifiablesPath    = s"${outputPath}training-data-classifiables"
	lazy val trainingDataPath                 = s"${outputPath}training-data"
	lazy val trainingDataPagesPath            = s"${outputPath}training-data-pages"
	lazy val bestPracticesPath                = s"${outputPath}best-practices.wiki"
	lazy val debug1Path                       = s"${outputPath}debug1.wiki"
	lazy val debug2Path                       = s"${outputPath}debug2.wiki"

	lazy val surfaceEvaluationPerDocumentPath = s"${outputPath}surface-evaluation-per-document.wiki"
	lazy val surfaceEvaluationPerSubsetPath   = s"${outputPath}surface-evaluation-per-subset.wiki"
	lazy val surfaceEvaluationPath            = s"${outputPath}surface-evaluation.wiki"

	lazy val newYorkTimesDataPath             = s"hdfs://tenemhead2/home/stefan.bunk/nyt/nyt.2007.tsv"
	lazy val classificationPath               = s"${outputPath}classification.wiki"
	lazy val inputDocumentsPath               = s"${outputPath}input-documents.wiki"
	lazy val fullNeighboursPath               = s"${outputPath}full-neighbours.wiki"
	lazy val reciprocalNeighboursPath         = s"${outputPath}reciprocal-neighbours.wiki"
	lazy val randomWalkResultsPath            = s"${outputPath}random-walk-results.wiki"
	lazy val rawFeaturesPath                  = s"${outputPath}raw-features.wiki"
	lazy val trieHitPath                      = s"${outputPath}trie-hits.wiki"

	lazy val pageRankPath                     = s"${outputPath}page-rank.wiki"

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
		val sb = new StringBuilder
		record.model.foreach { case (word, count) =>
			sb.append(s"$word=$count")
		}
	}
}
