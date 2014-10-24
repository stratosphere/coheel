package de.uni_potsdam.hpi.coheel.programs

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.core.fs.local.LocalFileSystem
import org.apache.flink.runtime.fs.hdfs.DistributedFileSystem
import de.uni_potsdam.hpi.coheel.wiki.{Link, Extractor, WikiPage, WikiPageReader}
import java.io._
import de.uni_potsdam.hpi.coheel.{PerformanceTimer, FlinkProgramRunner}
import org.apache.log4j.Logger

/**
 * Helper object for reused parts of Flink programs.
 */
object ProgramHelper {

	val log = Logger.getLogger(getClass)

	val fileType = FlinkProgramRunner.config.getString("type")
	val dumpFile = new Path(FlinkProgramRunner.config.getString("base_path") +
		FlinkProgramRunner.config.getString("dump_file"))
	val fileSystem = if (fileType == "file") new LocalFileSystem else new DistributedFileSystem

	lazy val wikipediaFilesPath = if (dumpFile.isAbsolute) dumpFile.toUri.toString
	                              else dumpFile.makeQualified(new LocalFileSystem).toUri.toString

	def getReader(f: File): Reader = {
		new BufferedReader(new FileReader(f))
	}
	def getWikiPages(env: ExecutionEnvironment, count: Int = Int.MaxValue): DataSet[WikiPage] = {
		var remainingPageCount = count
//		val input = env.readTextFile("hdfs://tenemhead2/home/stefan.bunk/wikipediaFilesPath")
		val input = env.readTextFile(wikipediaFilesPath)
		input.flatMap { fileName =>
			PerformanceTimer.endTimeFirst("FIRST OPERATOR")
			PerformanceTimer.startTimeFirst("WIKIPARSE-OPERATOR")
			val reader = new InputStreamReader(fileSystem.open(new Path(dumpFile.getParent, fileName)))
			val wikiPages = WikiPageReader.xmlToWikiPages(reader)
			val filteredWikiPages = wikiPages.filter { page =>
				val filter = page.ns == 0 && page.source.nonEmpty && remainingPageCount > 0
				remainingPageCount -= 1
				filter
			}
			val result = filteredWikiPages.take(remainingPageCount).flatMap { wikiPage =>
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
						log.error(s"${e.getClass.getSimpleName} in ${wikiPage.pageTitle}, ${e.getMessage}, ${e.getStackTraceString}")
						None
				}
				parsedWikiPage
			}
			PerformanceTimer.endTimeLast("WIKIPARSE-OPERATOR")
			result
		}.name("Wiki-Pages")
	}

	def filterNormalPages(wikiPages: DataSet[WikiPage]): DataSet[WikiPage] = {
		wikiPages.filter { wikiPage =>
			PerformanceTimer.startTimeFirst("WIKIPAGEFILTER-OPERATOR")
			val result = !wikiPage.isDisambiguation && !wikiPage.isRedirect && !wikiPage.isList
			PerformanceTimer.endTimeLast("WIKIPAGEFILTER-OPERATOR")
			result
		}
	}
}
