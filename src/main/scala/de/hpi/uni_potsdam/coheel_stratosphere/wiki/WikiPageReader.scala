package de.hpi.uni_potsdam.coheel_stratosphere.wiki

import scala.xml.Elem
import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.WikiTitle
import org.dbpedia.extraction.util.Language
import de.hpi.uni_potsdam.coheel_stratosphere.wiki.wikiparser.SimpleWikiParser

object WikiPageReader {
	/**
	 * @param elem The xml root element.
	 * @return A tuple of the pages title and the page's plain text content.
	 */
	def xmlToPlainText(elem: Elem): (String, String) = {
		val wikiPage = xmlToWikiPages(elem).next()
		val wikiParser = new SimpleWikiParser()
		val ast = wikiParser.apply(wikiPage)
		(wikiPage.title.decodedWithNamespace, ast.toPlainText)
	}


	def xmlToWikiPages(xml: Elem): Iterator[WikiPage] = {
		val pages = (xml \\ "page").iterator

		if (pages.isEmpty)
			throw new RuntimeException("No wikipage found!")
		new Iterator[WikiPage] {
			def hasNext = pages.hasNext
			def next(): WikiPage = {
				val page = pages.next()
				val rev = page \  "revision"

				val contributorID = rev \ "contributor" \ "id"
				val contributorName = rev \ "contributor" \ "username"

				new WikiPage(
					title           = WikiTitle.parse((page \ "title").head.text, Language.English),
					redirect        = null,
					id              = (page \ "id").head.text,
					revision        = (rev \ "id").head.text,
					timestamp       = (rev \ "timestamp").head.text,
					contributorID   = if (contributorID == null || contributorID.length != 1)
						"0" else contributorID.head.text,
					contributorName = if (contributorName == null || contributorName.length != 1)
						"" else contributorName.head.text,
					source          = (rev \ "text").text,
					format          = "text/x-wiki"
				)
			}
		}
	}

}
