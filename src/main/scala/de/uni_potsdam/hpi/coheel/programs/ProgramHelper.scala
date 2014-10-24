package de.uni_potsdam.hpi.coheel.programs

import org.apache.flink.api.scala._
import org.slf4s.Logging
import de.uni_potsdam.hpi.coheel.wiki.{Link, Extractor, WikiPage, WikiPageReader}
import java.io.{BufferedReader, FileReader, Reader, File}
import de.uni_potsdam.hpi.coheel.{PerformanceTimer, FlinkProgramRunner}

/**
 * Helper object for reused parts of Flink programs.
 */
object ProgramHelper extends Logging {

	val dumpFile = new File(FlinkProgramRunner.config.getString("base_path") + FlinkProgramRunner.config.getString("dump_file"))

	lazy val wikipediaFilesPath = s"file://${dumpFile.getAbsolutePath}"

	def getReader(f: File): Reader = {
		new BufferedReader(new FileReader(f))
	}
	def getWikiPages(env: ExecutionEnvironment, count: Int = Int.MaxValue): DataSet[WikiPage] = {
		var remainingPageCount = count
		val input = env.readTextFile(wikipediaFilesPath)
		input.flatMap { fileName =>
			PerformanceTimer.endTimeFirst("FIRST OPERATOR")
			val file = new File(s"${dumpFile.getAbsoluteFile.getParent}/$fileName")
			val wikiPages = WikiPageReader.xmlToWikiPages(getReader(file))
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
			result
		}.name("Wiki-Pages")
	}

	def filterNormalPages(wikiPages: DataSet[WikiPage]): DataSet[WikiPage] = {
		wikiPages.filter { wikiPage =>
			!wikiPage.isDisambiguation && !wikiPage.isRedirect && !wikiPage.isList
		}
	}
}
