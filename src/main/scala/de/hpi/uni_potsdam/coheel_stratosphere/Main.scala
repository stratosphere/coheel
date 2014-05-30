package de.hpi.uni_potsdam.coheel_stratosphere

import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.util.WikiApi
import java.net.URL
import org.dbpedia.extraction.util.Language
import de.hpi.uni_potsdam.coheel_stratosphere.wiki.LinkExtractor
import eu.stratosphere.client.LocalExecutor
import org.slf4s.Logging

object Main extends App with Logging {
	override def main(args: Array[String]): Unit = {
//		runExtraction()
		val task = new WikipediaTrainingTask()
		LocalExecutor.setOverwriteFilesByDefault(true)
		LocalExecutor.execute(task)
	}

	private def runExtraction(): Unit = {
		// Wikipedia-API-call:
		// http://en.wikipedia.org/w/api.php?action=query&pageids=11867&prop=revisions&rvprop=ids|content|timestamp|user|userid&format=xml
		val page = getExampleWikiPage(11867); // Germany
		val linkExtractor = new LinkExtractor()
		linkExtractor.extractLinks(page).foreach { link =>
			println(String.format("%80s||%s", link.text, link.destination))
		}

		// TODO:
		// Lucene Stemming/Tokenization
		//
	}

	private def getExampleWikiPage(pageId: Long): WikiPage = {
		val wikiApi = new WikiApi(
			new URL("http://en.wikipedia.org/w/api.php"),
			Language.English)
		val pageIds = List(pageId)
		val wikiPage = wikiApi.retrievePagesByPageID(pageIds).toList.head
		wikiPage
	}
}
