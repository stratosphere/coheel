package de.hpi.uni_potsdam.coheel_stratosphere

import org.scalatest.FunSuite
import scala.io.Source
import scala.xml.{Elem, XML}
import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.WikiTitle
import org.dbpedia.extraction.util.{WikiApi, Language}

class LinkExtractorTest extends FunSuite {

	lazy val source = getClass.getResource("/wikipedia_test_article.xml")
	def fixture = (new LinkExtractor(), buildPage())

	def buildPage(): WikiPage = {
		val xml = XML.load(source)
		// WikiApi is used to download a page from a given URL,
		// however, it also has Wiki-XML parsing built-in.
		val api = new WikiApi(null, null)

//		for(page <- xml \ "query" \ "pages" \ "page";
//		    rev <- page \ "revisions" \ "rev" ) {
//			System.out.println((page \ "@title").head.text)
//			WikiTitle.parse((page \ "@title").head.text, Language.English)
//		}

		val wikiPage: WikiPage = toWikiPage(xml)
//		api.processPages(xml, { page =>
//			wikiPage = page
//		})
//		xml.child.foreach { node =>
//			println(node.label)
//		}

		wikiPage
	}

	def toWikiPage(xml: Elem): WikiPage = {
		for (page <- xml \ "query" \ "pages" \ "page";
		    rev <- page \ "revisions" \ "rev") {
			val _contributorID = (rev \ "@userid");
			val _contributorName = (rev \ "@user");
			val _format = (rev \ "@contentformat");

			return new WikiPage(
				title           = WikiTitle.parse((page \ "@title").head.text, Language.English),
				redirect        = null,
				id              = (page \ "@pageid").head.text,
				revision        = (rev \ "@revid").head.text,
				timestamp       = (rev \ "@timestamp").head.text,
				contributorID   = if (_contributorID == null || _contributorID.length != 1) "0" else _contributorID.head.text,
				contributorName = if (_contributorName == null || _contributorName.length != 1) "" else _contributorName.head.text,
				source          = rev.text,
				format          = if (_format == null || _format.length != 1) "" else _format.head.text
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