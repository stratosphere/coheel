package de.uni_potsdam.hpi.coheel

import org.scalatest.FunSuite
import scala.xml.XML
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import de.uni_potsdam.hpi.coheel.wiki.{Link, Extractor, WikiPageReader}
import scala.io.Source

@RunWith(classOf[JUnitRunner])
class ExtractorTest extends FunSuite {

	def fixture() = new Extractor(wikiPage)

	lazy val wikiPage = {
		val source = getClass.getResource("/manual_test_files/wikipedia_Kilobyte.xml")
		val xml = Source.fromFile(source.toURI, "UTF-8").mkString
		WikiPageReader.xmlToWikiPages(xml).next()
	}

	def links: Seq[Link] = {
		val extractor = fixture()
		extractor.extractLinks()
	}

	test("parsing a simple internal link '[[byte]]'") {
		assert(links.exists { _.text == "byte" })
	}
	test("parsing a link whose text starts with an hash") {
		assert(links.exists { _.text == "#4 TOLL" })
	}
	test("parsing a link with template inside and starting with a blank") {
		assert(links.exists { _.text == "mortar" })
	}
	test("empty links are recognized") {
		assert(links.exists { link =>
			link.text == "Bilateral relations of Bosnia and Herzegovina" &&
				link.destination == "Bilateral relations of Bosnia and Herzegovina"
		})
	}
	test("parsing links with wiki markup") {
		assert(links.exists { link =>
			link.text == "Information theory" && link.destination == "Information theory"
		})
		assert(links.exists { link =>
			link.text == "\"Antwort\"" && link.destination == "wikt:antwort"
		})
	}
	test("parsing an internal link with different label '[[Computer data storage|digital information]]'") {
		assert(links.exists { link =>
			link.text == "digital information" && link.destination == "Computer data storage"
		})
	}
	test("parsing an internal link with anchor '[[Hertz#Computing|CPU clock speeds]]'") {
		assert(links.exists { link =>
			link.text == "CPU clock speeds" && link.destination == "Hertz"
		})
	}
	test("no categories, files or images are returned") {
		assert(!links.exists { link =>
			link.destination == ":Category:Information Units"
		})
		assert(links.forall { link =>
			!link.destination.startsWith("Category:") &&
				!link.destination.startsWith("Image:") &&
				!link.destination.startsWith("File:")
		})
	}
	test("no external link are returned") {
		assert(links.forall { link =>
			!link.destination.toLowerCase.startsWith("http://")
		})
	}

	test("all links are found (currently, we cannot find links in refs)") {
		assert(links.size === 54 /* hand-counted :) */)
	}

	test("just print links") {
		links.foreach { link =>
			println(String.format("%80s||%s", link.text, link.destination))
		}
	}

	test("does not run in infinite loop") {
		val source = getClass.getResource("/manual_test_files/infinite_loop.xml")
		val xml = Source.fromFile(source.toURI, "UTF-8").mkString
		val wikiPage = WikiPageReader.xmlToWikiPages(xml).next()
		val linkExtractor = new Extractor(wikiPage)
		linkExtractor.extractLinks()
	}

}
