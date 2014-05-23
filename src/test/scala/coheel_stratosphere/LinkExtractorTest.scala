package de.hpi.uni_potsdam.coheel_stratosphere

import org.scalatest.FunSuite
import scala.xml.{Elem, XML}
import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.WikiTitle
import org.dbpedia.extraction.util.{WikiApi, Language}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LinkExtractorTest extends FunSuite {

	lazy val source = getClass.getResource("/wikipedia_test_article.xml")
	def fixture = (new LinkExtractor(), buildPage())

	def buildPage(): WikiPage = {
		val xml = XML.load(source)
		// WikiApi is used to download a page from a given URL,
		// however, it also has Wiki-XML parsing built-in.
		val api = new WikiApi(null, null)

		xmlToWikiPage(xml)
	}

	def xmlToWikiPage(xml: Elem): WikiPage = {
		for (page <- xml \ "query" \ "pages" \ "page";
		    rev <- page \ "revisions" \ "rev") {
			val contributorID = rev \ "@userid"
			val contributorName = rev \ "@user"
			val format = rev \ "@contentformat"

			return new WikiPage(
				title           = WikiTitle.parse((page \ "@title").head.text, Language.English),
				redirect        = null,
				id              = (page \ "@pageid").head.text,
				revision        = (rev \ "@revid").head.text,
				timestamp       = (rev \ "@timestamp").head.text,
				contributorID   = if (contributorID == null || contributorID.length != 1) "0" else contributorID.head.text,
				contributorName = if (contributorName == null || contributorName.length != 1) "" else contributorName.head.text,
				source          = rev.text,
				format          = if (format == null || format.length != 1) "" else format.head.text
			)
		}
		null
	}

	test("Parsing a simple internal link works.") {
		val (extractor, wikiPage) = fixture

		val links = extractor.extractLinks(wikiPage)
		assert(links.exists { _.text == "byte" })
	}
}