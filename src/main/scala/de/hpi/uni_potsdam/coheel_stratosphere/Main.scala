package de.hpi.uni_potsdam.coheel_stratosphere

import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.util.WikiApi
import java.net.URL
import org.dbpedia.extraction.util.Language

object Main extends App {
	override def main(args: Array[String]): Unit = {
		// Wikipedia-API-call:
		// http://en.wikipedia.org/w/api.php?action=query&pageids=11867&prop=revisions&rvprop=content
		val page = getExampleWikiPage(11867); // Germany
		val linkExtractor = new LinkExtractor()
		linkExtractor.extractLinks(page).foreach { link =>
			println(String.format("%80s||%s", link.text, link.destination))
		}
	}

	private def getExampleWikiPage(pageId: Long): WikiPage = {
		val wikiApi = new WikiApi(
			new URL("http://en.wikipedia.org/w/api.php"),
			Language.English)
		val pageIds = List(pageId)
		val wikiPage = wikiApi.retrievePagesByPageID(pageIds).toList.head
		return wikiPage
	}
}
