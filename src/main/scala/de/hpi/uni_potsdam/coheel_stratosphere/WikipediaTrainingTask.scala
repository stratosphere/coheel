package de.hpi.uni_potsdam.coheel_stratosphere

import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import eu.stratosphere.api.common.{Program, ProgramDescription, Plan}
import de.hpi.uni_potsdam.coheel_stratosphere.wiki.{WikiPageReader, LinkExtractor}
import scala.xml.XML
import scala.io.Source


class WikipediaTrainingTask extends Program with ProgramDescription {

	override def getPlan(args: String*): Plan = {
		val currentPath = System.getProperty("user.dir")
		val input = TextFile(s"file://$currentPath/src/test/resources/wikipedia_files.txt")
		val words = input.map { file =>
			Source.fromFile(file).mkString
		}.flatMap { text =>
			val extractor = new LinkExtractor()
			val links = extractor.extractLinks(WikiPageReader.xmlToWikiPage(XML.loadString(text)))
//			links.map { link => link.destination }
			links
		} map {
			(_, 1)
		}

		val counts = words.groupBy { case (word, _) => word }
					.reduce { (w1, w2) => (w1._1, w1._2 + w2._2) }
//
		val output = counts.write(s"file://$currentPath/testoutput/test", CsvOutputFormat())
		val plan = new ScalaPlan(Seq(output))

		plan

	}

	override def getDescription = "Training the model parameters for CohEEL."
}
