package de.uni_potsdam.hpi.coheel.programs

import java.net.InetAddress

import de.uni_potsdam.hpi.coheel.FlinkProgramRunner
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.io.{IteratorReader, WikiPageInputFormat}
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import de.uni_potsdam.hpi.coheel.util.Util
import de.uni_potsdam.hpi.coheel.wiki._
import org.apache.flink.api.common.ProgramDescription
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.core.fs.local.LocalFileSystem
import org.apache.flink.util.Collector
import org.apache.log4j.Logger

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object CoheelLogger {
	val log: Logger = Logger.getLogger(getClass)
}
object CoheelProgram {

	def runsOffline(): Boolean = {
		if (FlinkProgramRunner.config == null) {
			false
		} else {
			val fileType = FlinkProgramRunner.config.getString("type")
			fileType == "file"
		}
	}

	def workerId(): Int = {
		val addr = InetAddress.getLocalHost
		val hostname = addr.getHostName
		println(hostname)
		assert(hostname.contains("tenem"))
		val tenemId = hostname.slice(hostname.length - 2, hostname.length).toInt
		tenemId - 11
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

	def makeProgram(env: ExecutionEnvironment): Unit = {
		makeProgram(env, null.asInstanceOf[T])
	}
	def makeProgram(env: ExecutionEnvironment, param: T): Unit = {
		environment = env
		buildProgram(env, param)
	}

	private lazy val wikiInput = environment.readFile(new WikiPageInputFormat, wikipediaFilesPath)
	private def readRawWikiPages[S : TypeInformation : ClassTag](fun: Extractor => S, pageFilter: String => Boolean = _ => true): DataSet[S] = {
		wikiInput.mapPartition { (linesIt: Iterator[String], out: Collector[S]) =>
			val reader = new IteratorReader(List("<foo>").iterator ++ linesIt ++ List("</foo>").iterator)
			val wikiPages = new WikiPageReader().xmlToWikiPages(reader, pageFilter)
			val filteredWikiPages = wikiPages.filter { page =>
				page.ns == 0 && page.source.nonEmpty
			}
			filteredWikiPages.foreach { wikiPage =>
				Try {
					val extractor = new Extractor(wikiPage, s => TokenizerHelper.tokenize(s).mkString(" ") )
					extractor.extract()
					fun(extractor)
				} match {
					case Success(parsedPage) =>
						out.collect(parsedPage)
					case Failure(e) =>
						log.error(s"Discarding ${wikiPage.pageTitle} because of ${e.getClass.getSimpleName} (${e.getMessage.replace('\n', ' ')})")
//						log.warn(e.getStackTraceString)
				}
			}
		}.name("Raw-Wiki-Pages")
	}

	def readWikiPages: DataSet[WikiPage] = {
		readRawWikiPages { extractor =>
			val wikiPage = extractor.wikiPage
			val rawPlainText = extractor.getPlainText
			val tokens = TokenizerHelper.tokenize(rawPlainText)
			// TODO: extractor.getLinks.asMapOfRanges().values().asScala.toArray???
			WikiPage(wikiPage.pageTitle, wikiPage.ns, wikiPage.redirect,
				tokens, extractor.getLinks.asMapOfRanges().values().asScala.toArray, wikiPage.isDisambiguation, wikiPage.isList)
		}

	}

	def readWikiPagesWithFullInfo(pageFilter: String => Boolean): DataSet[FullInfoWikiPage] = {
		readRawWikiPages({ extractor =>
			val wikiPage = extractor.wikiPage

			val rawPlainText = extractor.getPlainText
//			link text offsets tell, where the links start in the raw plain text
			val linkTextOffsets = extractor.getLinks
			val tokenizerResult = TokenizerHelper.tokenizeWithPositionInfo(rawPlainText, linkTextOffsets)
			assert(tokenizerResult.getTags.size == tokenizerResult.getTags.size)
			FullInfoWikiPage(wikiPage.pageTitle, wikiPage.ns, wikiPage.redirect,
				tokenizerResult.getTokens, tokenizerResult.getTags, tokenizerResult.getLinkPositions, wikiPage.isDisambiguation, wikiPage.isList)
		}, pageFilter)
	}


	def filterNormalPages(wikiPages: DataSet[WikiPage]): DataSet[WikiPage] = {
		wikiPages.filter { wikiPage =>
			wikiPage.isNormalPage
		}.name("Filter-Normal-Pages")
	}

	def readPlainTexts: DataSet[Plaintext] = {
		environment.readTextFile(plainTextsPath).name("Plain-Texts").flatMap { line =>
			val split = line.split('\t')
			// TODO: Change, once we only have plaintext files with 3 entries
			if (split.length == 2)
				Some(Plaintext(split(0), split(1), "\0"))
			else if (split.length == 3)
				Some(Plaintext(split(0), split(1), split(2)))
			else
				None
		}.name("Parsed Plain-Texts")
	}
	def readSurfaces(subFile: String = ""): DataSet[String] = {
		environment.readTextFile(surfaceDocumentCountsPath + subFile).name("Subset of Surfaces")
			.flatMap(new RichFlatMapFunction[String, String] {
			override def flatMap(line: String, out: Collector[String]): Unit = {
				val split = line.split('\t')
				if (split.length == 3)
					out.collect(split(0))
				else {
					log.warn(s"Discarding '${split.deep}' because split size not correct")
					log.warn(line)
				}
			}
		}).name("Parsed Surfaces")
	}
//	def getScores(): DataSet[(String, Array[String])] = {
//		environment.readTextFile(scoresPath).name("Scores")
//		.map { line =>
//			val values = line.split('\t')
//			(values(1), values)
//		}
//	}
	def readSurfaceLinkProbs(subFile: String = ""): DataSet[(String, Float)] = {
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

	def readSurfaceDocumentCounts: DataSet[SurfaceAsLinkCount] = {
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


	def readSurfaceProbs(threshold: Double = 0.0): DataSet[SurfaceProb] = {
		environment.readTextFile(surfaceProbsPath).flatMap { line =>
			val split = line.split('\t')
			if (split.length > 1) {
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
		}.name("Read surface probs")
	}

	def readLanguageModels(): DataSet[LanguageModel] = {
		environment.readTextFile(languageModelsPath).flatMap { line =>
			val lineSplit = line.split('\t')
			val pageTitle = lineSplit(0)
			if (lineSplit.length < 2) {
				log.warn(s"$pageTitle not long enough: $line")
				None
			} else {
				val model = lineSplit(1).split(' ').flatMap { entrySplit =>
					val wordSplit = entrySplit.split('\0')
					if (wordSplit.length == 2)
						Some(wordSplit(0), wordSplit(1).toDouble)
					else
						None
				}.toMap
				Some(LanguageModel(pageTitle, model))
			}
		}.name("Reading language models")
	}

	def readContextLinks(): DataSet[ContextLink] = {
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
