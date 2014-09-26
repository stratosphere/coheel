package de.uni_potsdam.hpi.coheel.programs

import org.apache.flink.api.scala.{TextFile, DataSet}
import org.slf4s.Logging
import scala.io.Source
import de.uni_potsdam.hpi.coheel.wiki.{Extractor, WikiPage, WikiPageReader}
import java.io.{BufferedReader, FileReader, Reader, File}
import de.uni_potsdam.hpi.coheel.FlinkProgramRunner
import DataSetNaming._

/**
 * Helper object for reused parts of Flink programs.
 */
object ProgramHelper extends Logging {

	val dumpFile = new File(FlinkProgramRunner.config.getString("base_path") + FlinkProgramRunner.config.getString("dump_file"))

	lazy val wikipediaFilesPath = s"file://${dumpFile.getAbsolutePath}"

	def getReader(f: File): Reader = {
		new BufferedReader(new FileReader(f))
	}

	def getWikiPages(count: Int = Int.MaxValue): DataSet[WikiPage] = {
		var remainingPageCount = count
		val input = TextFile(wikipediaFilesPath).name("Input-Text-Files")
		input.flatMap { fileName =>
			val file = new File(s"${dumpFile.getAbsoluteFile.getParent}/$fileName")
			val wikiPages = WikiPageReader.xmlToWikiPages(getReader(file))
			val filteredWikiPages = wikiPages.filter { page =>
				val filter = page.ns == 0 && page.source.nonEmpty && remainingPageCount > 0
				remainingPageCount -= 1
				filter
			}
			val result = filteredWikiPages.take(remainingPageCount).map { wikiPage =>
				try {
					val extractor = new Extractor(wikiPage)
					wikiPage.links = extractor.extractLinks()
					wikiPage.plainText = extractor.extractPlainText()
					wikiPage.source = ""
				} catch {
					case e: Throwable =>
						log.error(s"Error in ${wikiPage.pageTitle}, ${e.getMessage}, ${e.getStackTrace}")
				}
				wikiPage
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
