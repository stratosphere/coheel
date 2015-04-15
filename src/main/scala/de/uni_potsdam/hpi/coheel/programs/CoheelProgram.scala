package de.uni_potsdam.hpi.coheel.programs

import java.lang.Iterable

import de.uni_potsdam.hpi.coheel.FlinkProgramRunner
import de.uni_potsdam.hpi.coheel.io.{IteratorReader, WikiPageInputFormat}
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import de.uni_potsdam.hpi.coheel.wiki.{Extractor, TokenizerHelper, WikiPage, WikiPageReader}
import org.apache.flink.api.common.ProgramDescription
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapPartitionFunction}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.core.fs.Path
import org.apache.flink.core.fs.local.LocalFileSystem
import org.apache.flink.util.Collector
import org.apache.log4j.Logger
import de.uni_potsdam.hpi.coheel.io.OutputFiles._

import scala.collection.JavaConverters._

object CoheelLogger {
	val log: Logger = Logger.getLogger(getClass)
}
object CoheelProgram {

	def runsOffline(): Boolean = {
		val fileType = FlinkProgramRunner.config.getString("type")
		fileType == "file"
	}

	val LINK_SPLITTER = "\0"
}
abstract class CoheelProgram[T]() extends ProgramDescription {

	import CoheelLogger._
	lazy val dumpFile = new Path(FlinkProgramRunner.config.getString("base_path"))
	lazy val wikipediaFilesPath = if (dumpFile.isAbsolute) dumpFile.toUri.toString
		else dumpFile.makeQualified(new LocalFileSystem).toUri.toString

	var environment: ExecutionEnvironment = null

	val params: Seq[T]

	var configurationParams: Map[String, String] = _

	def buildProgram(env: ExecutionEnvironment, param: T): Unit

	def makeProgram(env: ExecutionEnvironment, param: T): Unit = {
		environment = env
		buildProgram(env, param)
	}

	def getWikiPages: DataSet[WikiPage] = {
		val input = environment.readFile(new WikiPageInputFormat, wikipediaFilesPath)

		input.mapPartition(new RichMapPartitionFunction[String, WikiPage] {
			override def mapPartition(linesIt: Iterable[String], out: Collector[WikiPage]): Unit = {
				val reader = new IteratorReader(List("<foo>").iterator ++ linesIt.iterator.asScala ++ List("</foo>").iterator)
				val wikiPages = new WikiPageReader().xmlToWikiPages(reader)
				val filteredWikiPages = wikiPages.filter { page =>
					val filter = page.ns == 0 && page.source.nonEmpty
					filter
				}
				filteredWikiPages.foreach { wikiPage =>
					val CONTEXT_SIZE = 50
					try {
						val extractor = new Extractor(wikiPage, s => TokenizerHelper.tokenize(s).mkString(" ") )
						extractor.extract()
						val linkTextOffsets = extractor.getLinks
						val (plainText, linkOffsets) = TokenizerHelper.tokenizeWithPositionInfo(extractor.getPlainText, linkTextOffsets)
						val linksWithContext = linkOffsets.map { case (position, link) =>
							val context = plainText.slice(Math.max(0, position - CONTEXT_SIZE), Math.min(position + CONTEXT_SIZE, plainText.length))
							import link._
							LinkWithContext(surface, surfaceRepr, source, destination, id, context)
						}.toArray
//						linkOffsets.foreach { case (linkOffset, link) =>
//							val textFromLink = link.surfaceRepr.split(' ')(0)
//							val textFromPlainText = plainText(linkOffset)
//							assert(textFromLink == textFromPlainText)
//						}
						wikiPage.source = ""
						out.collect(WikiPage(wikiPage.pageTitle, wikiPage.ns, wikiPage.redirect,
							plainText, linksWithContext, wikiPage.isDisambiguation, wikiPage.isList))
					} catch {
						case e: Throwable =>
							log.warn(s"Discarding ${wikiPage.pageTitle} because of ${e.getClass.getSimpleName} (${e.getMessage})")
							log.warn(e.getStackTraceString)
					}
				}
			}
		}).name("Wiki-Pages")
	}

	def filterNormalPages(wikiPages: DataSet[WikiPage]): DataSet[WikiPage] = {
		wikiPages.filter { wikiPage =>
			wikiPage.isNormalPage
		}.name("Filter-Normal-Pages")
	}

	def getPlainTexts: DataSet[Plaintext] = {
		environment.readTextFile(plainTextsPath).name("Plain-Texts").flatMap { line =>
			val split = line.split('\t')
			// TODO: Change, once we only have plaintext files with 3 entries
			if (split.size == 2)
				Some(Plaintext(split(0), split(1), "\0"))
			else if (split.size == 3)
				Some(Plaintext(split(0), split(1), split(2)))
			else
				None
		}.name("Parsed Plain-Texts")
	}
	def getSurfaces(subFile: String = ""): DataSet[String] = {
		environment.readTextFile(surfaceDocumentCountsPath + subFile).name("Subset of Surfaces")
			.flatMap(new RichFlatMapFunction[String, String] {
			override def flatMap(line: String, out: Collector[String]): Unit = {
				val split = line.split('\t')
				if (split.size == 3)
					out.collect(split(0))
				else {
					log.warn(s"Discarding '${split.deep}' because split size not correct")
					log.warn(line)
				}

			}
		}).name("Parsed Surfaces")
	}
	def getSurfaceLinkProbs(subFile: String = ""): DataSet[(String, Float)] = {
		environment.readTextFile(surfaceLinkProbsPath + subFile).name("Subset of Surfaces with Probabilities")
			.flatMap(new RichFlatMapFunction[String, (String, Float)] {
			override def flatMap(line: String, out: Collector[(String, Float)]): Unit = {
				val split = line.split('\t')
				if (split.length == 4)
					out.collect((split(0), split(3).toFloat))
				else {
					log.warn(s"Discarding '${split.deep}' because split size not correct")
					log.warn(line)
				}

			}
		}).name("Parsed Surfaces with Probabilities")
	}

	def getSurfaceDocumentCounts: DataSet[SurfaceAsLinkCount] = {
		environment.readTextFile(surfaceDocumentCountsPath).name("Raw-Surface-Document-Counts").flatMap { line =>
			val split = line.split('\t')
			// not clear, why lines without a count occur, but they do
			try {
				if (split.length != 3) {
					log.warn(s"Discarding '${split.deep}' because split size not correct")
					log.warn(line)
					None
				} else {
					val (surface, count) = (split(0), split(2).toInt)
					Some(SurfaceAsLinkCount(surface, count))
				}
			} catch {
				case e: NumberFormatException =>
					log.warn(s"Discarding '${split(0)}' because of ${e.getClass.getSimpleName}")
					log.warn(line)
					None
			}
		}.name("Surface-Document-Counts")
	}


	def getSurfaceProbs(threshold: Double = 0.0): DataSet[SurfaceProb] = {
		environment.readTextFile(surfaceProbsPath).flatMap { line =>
			val split = line.split('\t')
			if (split.size > 1) {
				val tokens = split(0).split(' ')
				if (tokens.nonEmpty) {
					val prob = split(2).toDouble
					if (prob > threshold)
						Some(SurfaceProb(split(0), split(1), prob))
					else
						None
				}
				else
					None
			}
			else None
		}
	}

	def getContextLinks(): DataSet[ContextLink] = {
		environment.readTextFile(contextLinkProbsPath).map { line =>
			val split = line.split('\t')
			ContextLink(split(0), split(1), split(2).toDouble)
		}
	}

	def runsOffline(): Boolean = {
		CoheelProgram.runsOffline()
	}
}

abstract class NoParamCoheelProgram extends CoheelProgram[Void] {
	val params: Seq[Void] = List(null)

	def buildProgram(env: ExecutionEnvironment): Unit
	override def buildProgram(env: ExecutionEnvironment, param: Void): Unit = {
		buildProgram(env)
	}
}
