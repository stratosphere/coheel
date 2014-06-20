package de.hpi.uni_potsdam.coheel_stratosphere

import org.scalatest.FunSuite
import scala.xml.XML
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import de.hpi.uni_potsdam.coheel_stratosphere.wiki.{Link, LinkExtractor, WikiPageReader}

@RunWith(classOf[JUnitRunner])
class LinkExtractorTest extends FunSuite {

	def fixture = (new LinkExtractor(), wikiPage)

	lazy val wikiPage = {
		val source = getClass.getResource("/wikipedia_Kilobyte.xml")
		val xml = XML.load(source)
		WikiPageReader.xmlToWikiPages(xml).next()
	}

	def links: Seq[Link] = {
		val (extractor, wikiPage) = fixture
		extractor.extractLinks(wikiPage)
	}

	test("parsing a simple internal link '[[byte]]'") {
		assert(links.exists { _.text == "byte" })
	}
	test("parsing a link whose text starts with an hash") {
		assert(links.exists { _.text == "#4 TOLL" })
	}
	test("parsing an internal link with different label '[[Computer data storage|digital information]]'") {
		assert(links.exists { link =>
			link.text == "digital information" && link.destinationPage == "Computer data storage"
		})
	}
	test("parsing an internal link with anchor '[[Hertz#Computing|CPU clock speeds]]'") {
		assert(links.exists { link =>
			link.text == "CPU clock speeds" && link.destinationPage == "Hertz"
		})
	}
	test("no categories, files or images are returned") {
		assert(!links.exists { link =>
			link.destinationPage == ":Category:Information Units"
		})
		assert(links.forall { link =>
			!link.destinationPage.startsWith("Category:") &&
				!link.destinationPage.startsWith("Image:") &&
				!link.destinationPage.startsWith("File:")
		})
	}
	test("no external link are returned") {
		assert(links.forall { link =>
			!link.destinationPage.toLowerCase.startsWith("http://")
		})
	}

	test("all links are found") {
		assert(links.size === 53 /* hand-counted :) */)
	}

	test("just print links") {
		links.foreach { link =>
//			println(String.format("%80s||%s", link.text, link.destinationPage))
		}
	}

}