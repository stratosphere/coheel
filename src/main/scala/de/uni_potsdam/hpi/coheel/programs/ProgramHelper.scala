package de.uni_potsdam.hpi.coheel.programs

import eu.stratosphere.api.scala.{DataSet, TextFile}
import org.slf4s.Logging
import scala.io.Source
import de.uni_potsdam.hpi.coheel.wiki.{WikiPage, WikiPageReader}
import java.io.File
import de.uni_potsdam.hpi.coheel.Main

object ProgramHelper extends Logging {

	val dumpFile = new File(Main.config.getString("base_path") + Main.config.getString("dump_file"))

	lazy val wikipediaFilesPath = s"file://${dumpFile.getAbsolutePath}"

	def getWikiPages: DataSet[WikiPage] = {
		val input = TextFile(wikipediaFilesPath)
		input.map { file =>
			log.info(file)
			val pageSource = Source.fromFile(s"${dumpFile.getAbsoluteFile.getParent}/$file").mkString
			pageSource
		}.flatMap { pageSource =>
			if (pageSource.startsWith("#")) {
				List[WikiPage]().iterator
			} else {
				val wikiPages = WikiPageReader.xmlToWikiPages(pageSource)
				wikiPages.filter { page => page.ns == 0}
			}
		}
	}
}
