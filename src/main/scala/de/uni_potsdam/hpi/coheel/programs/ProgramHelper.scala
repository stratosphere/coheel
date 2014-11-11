package de.uni_potsdam.hpi.coheel.programs

import java.net.InetSocketAddress

import com.typesafe.config.ConfigFactory
import de.uni_potsdam.hpi.coheel.io.WikiPageInputFormat
import org.apache.hadoop.fs
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.core.fs.local.LocalFileSystem
import org.apache.flink.runtime.fs.hdfs.DistributedFileSystem
import de.uni_potsdam.hpi.coheel.wiki.{Link, Extractor, WikiPage, WikiPageReader}
import java.io._
import de.uni_potsdam.hpi.coheel.{PerformanceTimer, FlinkProgramRunner}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger

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
//		val input = env.readTextFile("hdfs://tenemhead2/home/stefan.bunk/wikipediaFilesPath")
//		val input = env.readTextFile(wikipediaFilesPath)
		val input = env.readFile(new WikiPageInputFormat, wikipediaFilesPath)

		input.flatMap { linesIt =>
			if (linesIt.contains("<xml_split:root xmlns:xml_split")) {
				List()
			} else {
				val fileContent = "<foo><page>" + linesIt.replace("</xml_split:root>", "") + "</foo>"
//				println("================================")
//				println(fileContent)
				val reader = new StringReader(fileContent)
				val wikiPages = new WikiPageReader().xmlToWikiPages(reader)
				val filteredWikiPages = wikiPages.filter { page =>
					val filter = page.ns == 0 && page.source.nonEmpty
					filter
				}
				val result = filteredWikiPages.flatMap { wikiPage =>
					val parsedWikiPage = try {
						val extractor = new Extractor(wikiPage)
						val alternativeNames = extractor.extractAlternativeNames().map {
							Link(wikiPage.pageTitle, _, wikiPage.pageTitle)
						}
						val links = alternativeNames ++ extractor.extractLinks()
						val plainText = extractor.extractPlainText()
						wikiPage.source = ""
						Some(WikiPage(wikiPage.pageTitle, wikiPage.ns, wikiPage.redirect,
							plainText, links.toArray, wikiPage.isDisambiguation, wikiPage.isList))
					} catch {
						case e: Throwable =>
							println(s"${e.getClass.getSimpleName} in ${wikiPage.pageTitle}, ${e.getMessage}, ${e.getStackTraceString}")
							None
					}
					parsedWikiPage
				}
				result
			}
		}.name("Wiki-Pages")
	}

	def filterNormalPages(wikiPages: DataSet[WikiPage]): DataSet[WikiPage] = {
		wikiPages.filter { wikiPage =>
			val result = !wikiPage.isDisambiguation && !wikiPage.isRedirect && !wikiPage.isList
			result
		}.name("Filter-Normal-Pages")
	}
}
