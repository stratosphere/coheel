package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.FlinkProgramRunner
import de.uni_potsdam.hpi.coheel.wiki.WikiPage
import org.apache.flink.api.common.ProgramDescription
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.log4j.Logger
import java.lang.Iterable
import java.util.Date

import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.io.{IteratorReader, WikiPageInputFormat}
import org.apache.flink.api.common.functions.RichMapPartitionFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.core.fs.local.LocalFileSystem
import de.uni_potsdam.hpi.coheel.wiki.{TokenizerHelper, Extractor, WikiPage, WikiPageReader}
import java.io._
import de.uni_potsdam.hpi.coheel.FlinkProgramRunner
import org.apache.flink.util.Collector
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.log4j.Logger
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
