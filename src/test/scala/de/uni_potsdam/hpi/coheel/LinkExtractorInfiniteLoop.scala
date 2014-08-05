package de.uni_potsdam.hpi.coheel

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import de.uni_potsdam.hpi.coheel.wiki.{WikiPageReader, LinkExtractor}
import scala.io.Source

@RunWith(classOf[JUnitRunner])
class LinkExtractorInfiniteLoop extends FunSuite {

	test("infiniteness") {
		val source = getClass.getResource("/manual_test_files/infinite_loop.xml")
		val xml = Source.fromFile(source.toURI, "UTF-8").mkString
		val wikiPage = WikiPageReader.xmlToWikiPages(xml).next()
		val linkExtractor = new LinkExtractor()
		linkExtractor.extractLinks(wikiPage)
	}
}
