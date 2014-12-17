package de.uni_potsdam.hpi.coheel

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import de.uni_potsdam.hpi.coheel.wiki.{Extractor, WikiPageReader}
import scala.io.Source
import de.uni_potsdam.hpi.coheel.programs.DataClasses._

@RunWith(classOf[JUnitRunner])
class ExtractorTest extends FunSuite {

	def fixture() = new Extractor(wikiPage)

	lazy val wikiPage = {
		val source = getClass.getResource("/manual_test_files/wikipedia_Kilobyte.xml")
		val xml = Source.fromFile(source.toURI, "UTF-8").mkString
		new WikiPageReader().xmlToWikiPages(xml).next()
	}

	def links: Seq[Link] = {
		val extractor = fixture()
		extractor.extractLinks()
		val alternativeNames = extractor.extractAlternativeNames()
		extractor.extractLinks() ++ alternativeNames
	}

	test("Check links inside of ''") {
		assert(links.exists { link => link.surface == "Anno Domini" && link.destination == "Anno Domini" })
	}

	test("remove quotes around links") {
		assert(links.exists { _.surface == "Alternatives to common law systems" })
		assert(links.exists { _.surface == "\"ABC\" (song)" })
		assert(links.exists { _.surface == "\"" })
	}
	test("parsing bold text at beginning of article") {
		assert(links.exists { _.surface == "Aldrovandia phalacra" })
		assert(links.exists { _.surface == "Hawaiian halosaurid" })
	}
	test("parsing a simple internal link '[[byte]]'") {
		assert(links.exists { _.surface == "byte" })
	}
	test("parsing a link whose text starts with an hash") {
		assert(links.exists { _.surface == "#4 TOLL" })
	}
	test("parsing a link with template inside and starting with a blank") {
		assert(links.exists { _.surface == "mortar" })
	}
	test("empty links are recognized") {
		assert(links.exists { link =>
			link.surface == "Bilateral relations of Bosnia and Herzegovina" &&
				link.destination == "Bilateral relations of Bosnia and Herzegovina"
		})
	}
	test("parsing links with wiki markup") {
		assert(links.exists { link =>
			link.surface == "Information theory" && link.destination == "Information theory"
		})
		assert(links.exists { link =>
			link.surface == "Antwort" && link.destination == "wikt:antwort"
		})
	}
	test("parsing an internal link with different label '[[Computer data storage|digital information]]'") {
		assert(links.exists { link =>
			link.surface == "digital information" && link.destination == "Computer data storage"
		})
	}
	test("parsing an internal link with anchor '[[Hertz#Computing|CPU clock speeds]]'") {
		assert(links.exists { link =>
			link.surface == "CPU clock speeds" && link.destination == "Hertz"
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
		assert(links.size === 60 /* hand-counted :) */)
	}

	test("just print links") {
		links.foreach { link =>
//			println(String.format("%80s||%s", link.surface, link.destination))
		}
	}

	test("does not run in infinite loop") {
		val source = getClass.getResource("/manual_test_files/infinite_loop.xml")
		val xml = Source.fromFile(source.toURI, "UTF-8").mkString
		val wikiPage = new WikiPageReader().xmlToWikiPages(xml).next()
		val linkExtractor = new Extractor(wikiPage)
		linkExtractor.extractLinks()
	}

}
