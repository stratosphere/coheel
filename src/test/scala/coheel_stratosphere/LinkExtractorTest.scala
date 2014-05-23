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

	def fixture = (new LinkExtractor(), wikiPage)

	lazy val wikiPage = {
		val source = getClass.getResource("/wikipedia_test_article.xml")
		val xml = XML.load(source)
		WikiPageReader.xmlToWikiPage(xml)
	}

	test("Parsing a simple internal link works.") {
		val (extractor, wikiPage) = fixture

		val links = extractor.extractLinks(wikiPage)
		assert(links.exists { _.text == "byte" })
	}
}