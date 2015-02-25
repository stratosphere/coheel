package de.uni_potsdam.hpi.coheel.programs

import java.lang.Iterable

import de.uni_potsdam.hpi.coheel.FlinkProgramRunner
import de.uni_potsdam.hpi.coheel.io.{IteratorReader, WikiPageInputFormat}
import de.uni_potsdam.hpi.coheel.programs.DataClasses.SurfaceAsLinkCount
import de.uni_potsdam.hpi.coheel.wiki.{Extractor, TokenizerHelper, WikiPage, WikiPageReader}
import org.apache.flink.api.common.ProgramDescription
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapPartitionFunction}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.core.fs.local.LocalFileSystem
import org.apache.flink.util.Collector
import org.apache.log4j.Logger
import de.uni_potsdam.hpi.coheel.io.OutputFiles._

import scala.collection.JavaConverters._

abstract class CoheelProgram[T]() extends ProgramDescription {

	@transient val log = Logger.getLogger(getClass)
	lazy val dumpFile = new Path(FlinkProgramRunner.config.getString("base_path"))
	lazy val wikipediaFilesPath = if (dumpFile.isAbsolute) dumpFile.toUri.toString
	else dumpFile.makeQualified(new LocalFileSystem).toUri.toString

	var environment: ExecutionEnvironment = null

	val params: Seq[T]

	var configurationParams: Map[String, String] = _

	def buildProgram(env: ExecutionEnvironment, param: T): Unit

	def makeProgram(env: ExecutionEnvironment, param: T): Unit = {
		environment = env
		buildProgram(env, param)
	}

	def getWikiPages(): DataSet[WikiPage] = {
		val input = environment.readFile(new WikiPageInputFormat, wikipediaFilesPath)

		input.mapPartition(new RichMapPartitionFunction[String, WikiPage] {
			override def mapPartition(linesIt: Iterable[String], out: Collector[WikiPage]): Unit = {
				val reader = new IteratorReader(List("<foo>").iterator ++ linesIt.iterator.asScala ++ List("</foo>").iterator)
				val wikiPages = new WikiPageReader().xmlToWikiPages(reader)
				val filteredWikiPages = wikiPages.filter { page =>
					val filter = page.ns == 0 && page.source.nonEmpty
					filter
				}
				filteredWikiPages.foreach { wikiPage =>
					try {
						val extractor = new Extractor(wikiPage, s => TokenizerHelper.transformToTokenized(s) )
						val links = extractor.extractAllLinks()
						val plainText = extractor.extractPlainText()
						wikiPage.source = ""
						out.collect(WikiPage(wikiPage.pageTitle, wikiPage.ns, wikiPage.redirect,
							plainText, links.toArray, wikiPage.isDisambiguation, wikiPage.isList))
					} catch {
						case e: Throwable =>
							println(s"${e.getClass.getSimpleName} in ${wikiPage.pageTitle}, ${e.getMessage}, ${e.getStackTraceString}")
							None
					}
				}
			}
		}).name("Wiki-Pages")
	}

	def filterNormalPages(wikiPages: DataSet[WikiPage]): DataSet[WikiPage] = {
		wikiPages.filter { wikiPage =>
			wikiPage.isNormalPage
		}.name("Filter-Normal-Pages")
	}

	def getPlainTexts(): DataSet[(String, String)] = {
		environment.readTextFile(plainTextsPath).name("Plain-Texts").flatMap { line =>
			val split = line.split('\t')
			if (split.size == 2)
				Some((split(0), split(1)))
			else
				None
		}.name("Parsed Plain-Texts")
	}
	def getSurfaces(subFile: String = ""): DataSet[String] = {
		environment.readTextFile(surfaceDocumentCountsPath + subFile).name("Subset of Surfaces")
			.flatMap(new RichFlatMapFunction[String, String] {
			override def open(params: Configuration): Unit = { }
			override def flatMap(line: String, out: Collector[String]): Unit = {
				val split = line.split('\t')
				if (split.size == 3)
					out.collect(split(0))
			}
		}).name("Parsed Surfaces")
	}

	def getSurfaceDocumentCounts(): DataSet[SurfaceAsLinkCount] = {
		environment.readTextFile(surfaceDocumentCountsPath).name("Raw-Surface-Document-Counts").map { line =>
			val split = line.split('\t')
			// not clear, why lines without a count occur, but they do
			try {
				if (split.size < 3)
					SurfaceAsLinkCount(split(0), 0)
				else {
					val (surface, count) = (split(0), split(2).toInt)
					SurfaceAsLinkCount(surface, count)
				}
			} catch {
				case e: NumberFormatException =>
					println(e)
					println(line)
					SurfaceAsLinkCount(split(0), 0)
			}
		}.name("Surface-Document-Counts")
	}

	def runsOffline(): Boolean = {
		val fileType = FlinkProgramRunner.config.getString("type")
		fileType == "file"
	}
}

abstract class NoParamCoheelProgram extends CoheelProgram[Void] {
	val params: Seq[Void] = List(null)

	def buildProgram(env: ExecutionEnvironment): Unit
	override def buildProgram(env: ExecutionEnvironment, param: Void): Unit = {
		buildProgram(env)
	}
}
