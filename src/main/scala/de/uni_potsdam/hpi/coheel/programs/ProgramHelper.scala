package de.uni_potsdam.hpi.coheel.programs

import java.lang.Iterable

import de.uni_potsdam.hpi.coheel.io.{IteratorReader, WikiPageInputFormat}
import org.apache.flink.api.common.functions.RichMapPartitionFunction
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.core.fs.local.LocalFileSystem
import org.apache.flink.runtime.fs.hdfs.DistributedFileSystem
import de.uni_potsdam.hpi.coheel.wiki.{Extractor, WikiPage, WikiPageReader}
import java.io._
import de.uni_potsdam.hpi.coheel.FlinkProgramRunner
import org.apache.flink.util.Collector
import org.apache.log4j.Logger
import scala.collection.JavaConverters._

/**
 * Helper object for reused parts of Flink programs.
 */
object ProgramHelper {

	val log = Logger.getLogger(getClass)


	lazy val fileType = FlinkProgramRunner.config.getString("type")
	lazy val dumpFile = new Path(FlinkProgramRunner.config.getString("base_path"))
	val fileSystem = if (fileType == "file") new LocalFileSystem else new DistributedFileSystem
	lazy val wikipediaFilesPath = if (dumpFile.isAbsolute) dumpFile.toUri.toString
	                              else dumpFile.makeQualified(new LocalFileSystem).toUri.toString

	def getReader(f: File): Reader = {
		new BufferedReader(new FileReader(f))
	}

	def getWikiPages(env: ExecutionEnvironment): DataSet[WikiPage] = {
		val input = env.readFile(new WikiPageInputFormat, wikipediaFilesPath)

		input.mapPartition(new RichMapPartitionFunction[String, WikiPage] {
			override def mapPartition(linesIt: Iterable[String], out: Collector[WikiPage]): Unit = {
//				val fileContent = "<foo>" + linesIt.mkString("\n") + "</foo>"
//				val reader = new StringReader(fileContent)
				val reader = new IteratorReader(List("<foo>").iterator ++ linesIt.iterator.asScala ++ List("</foo>").iterator)
				val wikiPages = new WikiPageReader().xmlToWikiPages(reader)
				val filteredWikiPages = wikiPages.filter { page =>
					val filter = page.ns == 0 && page.source.nonEmpty
					filter
				}
				filteredWikiPages.foreach { wikiPage =>
					try {
						val extractor = new Extractor(wikiPage)
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
}
