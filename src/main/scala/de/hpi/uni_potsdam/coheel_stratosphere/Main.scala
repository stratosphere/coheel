package de.hpi.uni_potsdam.coheel_stratosphere

import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.util.WikiApi
import java.net.URL
import org.dbpedia.extraction.util.Language
import de.hpi.uni_potsdam.coheel_stratosphere.wiki.{WikiPageReader, LinkExtractor}
import org.slf4s.Logging
import eu.stratosphere.client.LocalExecutor
import scala.xml.XML

object Main extends App with Logging {

	/**
	 * Open tasks:
	 * <ul>
	 *   <li> Use a trie for NER
	 *   <li> Wikipedia, handle disambiguation sites, handle list sites
	 *   <li> Compact language model
	 */
	override def main(args: Array[String]): Unit = {
//		val elem = XML.loadFile("src/test/resources/enwiki-latest-pages-articles1.xml-p000000010p000010000")
//		println(WikiPageReader.xmlToWikiPages(elem).next())

		val task = new WikipediaTrainingTask()
		LocalExecutor.setOverwriteFilesByDefault(true)
		LocalExecutor.execute(task)
	}
}
