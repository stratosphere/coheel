package de.hpi.uni_potsdam.coheel_stratosphere.wiki

import scala.xml.Elem
import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.WikiTitle
import org.dbpedia.extraction.util.Language
import de.hpi.uni_potsdam.coheel_stratosphere.wiki.wikiparser.SimpleWikiParser


/**
 * Captures the important aspects of a WikiPage for our use case, while still
 * maintaning connection (via inheritance) to the DBpedia extraction framework.
 * <br />
 * <strong>Definition redirect</strong>:<br />
 * A page is seen as a redirect, if it contains the &lt;redirect&gt; tag inside the &lt;page&gt; tag in the wiki
 * dump.
 * <br />
 * <strong>Definition disambiguation</strong>:<br />
 * A page is seen as a disambiguation if one of the following conditions is true:
 * <ul>
 *     <li> The title contains "(disambiguation)"
 *     <li> The text contains a disambiguation template (not all disambiguation pages contain "(disambiguation)" in the
 *          title, e.g. http://en.wikipedia.org/wiki/Alien
 * @param pageTitle The title of the page as a string.
 * @param redirectTitle The title of the page this page is redirecting to or "", if it is not a redirect.
 * @param text This page's content.
 */
case class CoheelWikiPage(pageTitle: String, redirectTitle: String, text: String) extends WikiPage(
	title           = WikiTitle.parse(pageTitle, Language.English),
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
	def isDisambiguation: Boolean = {
		val isDisambiguationFromTitle = pageTitle.contains("(disambiguation)")
		if (isDisambiguationFromTitle)
			return true

		val disambiguationRegex = """(?ui)\{\{disambiguation.*?\}\}""".r
		val matches = disambiguationRegex.findAllIn(text)
			// check whether the regex sometimes accidentially matches to much text
			.map { s =>
				if (s.length > 60)
					throw new RuntimeException(s"Disambiguation regex went wrong on $s.")
				s
			}
			.map(_.toLowerCase)
			.filter { s =>
				!s.contains("disambiguation needed")
			}

		matches.nonEmpty
	}
}

object WikiPageReader {

	/**
	 * @param wikiPage
	 * @return A tuple of the pages title and the page's plain text content.
	 */
	def wikiPageToText(wikiPage: CoheelWikiPage): (String, String) = {
		val wikiParser = new SimpleWikiParser()
		val ast = wikiParser.apply(wikiPage)
		(wikiPage.pageTitle, ast.toPlainText)
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
					pageTitle = (page \ "title").head.text,
					redirectTitle = redirect.toString(),
					text  = (rev \ "text").text
				)
			}
		}
	}

}
