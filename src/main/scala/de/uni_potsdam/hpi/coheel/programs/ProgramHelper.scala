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

	def getWikiPages(count: Int = -1): DataSet[WikiPage] = {
		val input = TextFile(wikipediaFilesPath).name("Input-Text-Files")
		input.flatMap { fileName =>
			log.info(s"Reading $fileName")
			val file= new File(s"${dumpFile.getAbsoluteFile.getParent}/$fileName")
			val wikiPages = WikiPageReader.xmlToWikiPages(getReader(file))
			(if (count == -1) wikiPages else wikiPages.take(count))
				.filter { page => page.ns == 0 && page.source.nonEmpty }.map { wikiPage =>
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
		}.name("Wiki-Pages")
	}

	def filterNormalPages(wikiPages: DataSet[WikiPage]): DataSet[WikiPage] = {
		wikiPages.filter { wikiPage =>
			!wikiPage.isDisambiguation && !wikiPage.isRedirect && !wikiPage.isList
		}
	}

}
