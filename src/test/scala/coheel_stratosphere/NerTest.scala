package de.hpi.uni_potsdam.coheel_stratosphere

import org.scalatest.{BeforeAndAfterEach, FunSuite}
import scala.xml.XML
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import de.hpi.uni_potsdam.coheel_stratosphere.wiki.{TextAnalyzer, Link, LinkExtractor, WikiPageReader}
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree
import com.github.tototoshi.csv.{TSVFormat, CSVReader}
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory
import scala.collection.JavaConversions._
import eu.stratosphere.client.LocalExecutor

/**
 * Testing the Named Entity Recognition by using a trie datastructure, and using
 * the mentions found in the test set.
 */
@RunWith(classOf[JUnitRunner])
class NerTest extends FunSuite with BeforeAndAfterEach {

	def fixture = "fixture"

	override def beforeEach(): Unit = {
		val currentPath = System.getProperty("user.dir")
		val path = s"file://$currentPath/src/test/resources/wikipedia_files.txt"
		val task = new WikipediaTrainingTask(path)
		LocalExecutor.setOverwriteFilesByDefault(true)
		LocalExecutor.execute(task)
	}

	test("ner'") {
//		val tree = new ConcurrentRadixTree[Boolean](new DefaultCharArrayNodeFactory)
//		implicit object MyFormat extends TSVFormat { }
//		val lines = CSVReader.open("testoutput/surface-counts")
//		lines.foreach { line =>
//			tree.put(line(0), true)
//		}
//
//		val (_, plainText) = WikiPageReader.xmlToPlainText(XML.loadFile("src/test/resources/wikipedia_Angela_Merkel.xml"))
//		val tokens = new TextAnalyzer().tokenize(plainText, false)
//		tokens.sliding(3).foreach { window =>
//			1 to window.size foreach { n =>
//				val possibleMention = window.take(n).mkString(" ")
//				val mentionIterator = tree.getKeysStartingWith(possibleMention)
//				mentionIterator.foreach(println)
//			}
//		}
	}
}
