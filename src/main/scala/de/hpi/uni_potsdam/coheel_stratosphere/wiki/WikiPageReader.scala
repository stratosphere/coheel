package de.hpi.uni_potsdam.coheel_stratosphere.wiki

import scala.xml.Elem
import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.WikiTitle
import org.dbpedia.extraction.util.Language
import de.hpi.uni_potsdam.coheel_stratosphere.wiki.wikiparser.SimpleWikiParser


/**
 * Captures the important aspects of a WikiPage for our use case, while still
 * maintaning connection (via inheritance) to the DBpedia extraction framework.
 * @param title
 * @param text
 */
class CoheelWikiPage(title: String, redirectTitle: String, text: String) extends WikiPage(
	title           = WikiTitle.parse(title, Language.English),
	redirect        = if (redirectTitle == "") null else WikiTitle.parse(redirectTitle, Language.English),
	id              = 0,
	revision        = 0,
	timestamp       = 0,
	contributorID   = 0,
	contributorName = "",
	source          = text,
	format          = "text/x-wiki"
) {

	def isRedirect: Boolean = this.redirect != null
	def isDisambiguation: Boolean = false
}

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

	var i = 0
	def xmlToWikiPages(xml: Elem): Iterator[CoheelWikiPage] = {
		val pages = (xml \\ "page").iterator

		if (pages.isEmpty)
			throw new RuntimeException("No wikipage found!")
		new Iterator[CoheelWikiPage] {
			def hasNext = pages.hasNext
			def next(): CoheelWikiPage = {
				val page = pages.next()
				val rev = page \  "revision"
				val redirect = page \ "redirect" \ "@title"

				new CoheelWikiPage(
					title = (page \ "title").head.text,
					redirectTitle = redirect.toString(),
					text  = (rev \ "text").text
				)
			}
		}
	}

}
