package de.uni_potsdam.hpi.coheel

import de.uni_potsdam.hpi.coheel.wiki.{WikiPageReader, Extractor}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class PlainTextExtractionTest extends FunSuite {

	def fixture() = {
		val extractor = new Extractor(wikiPage, s => s)
		extractor.extract()
		extractor
	}

	lazy val wikiPage = {
		val source = getClass.getResource("/manual_test_files/wikipedia_Kilobyte.xml")
		val xml = Source.fromFile(source.toURI, "UTF-8").mkString
		new WikiPageReader().xmlToWikiPages(xml).next()
	}

	test("print plaintext") {
		println(fixture().getPlainText)

	}
	test("contains file captions") {
		val plainText = fixture().getPlainText
		assert(plainText.contains("University of Cambridge"))
	}

	test("contains extra paragraphs") {
		val plainText = fixture().getPlainText
		assert(plainText.contains("Empty texts with categories"))
	}

	test("contains broken links correctly") {
		val plainText = fixture().getPlainText
		assert(plainText.contains("Bilateral relations of Bosnia and Herzegovina"))

	}


}
