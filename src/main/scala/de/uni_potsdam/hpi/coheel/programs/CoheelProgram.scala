package de.uni_potsdam.hpi.coheel.programs

import java.io.{Reader, StringReader}
import java.net.InetAddress

import de.uni_potsdam.hpi.coheel.{Params, FlinkProgramRunner}
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.io.{OutputFiles, IteratorReader, WikiPageInputFormat}
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import de.uni_potsdam.hpi.coheel.util.Util
import de.uni_potsdam.hpi.coheel.wiki._
import edu.umd.cloud9.collection.wikipedia.WikipediaPageInputFormat
import org.apache.flink.api.common.ProgramDescription
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.hadoop.mapred.HadoopInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.core.fs.local.LocalFileSystem
import org.apache.flink.util.Collector
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.InputFormat
import org.apache.log4j.Logger
import edu.umd.cloud9.collection.wikipedia.WikipediaPage

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
			fileType == "local"
		}
	}

	val LINK_SPLITTER = "\0"
}
abstract class CoheelProgram[T]() extends ProgramDescription {

	import CoheelLogger._
	lazy val basePath = new Path(FlinkProgramRunner.config.getString("base_path"))
//	lazy val wikipediaDumpFilesPath = s"${OutputFiles.protocol}://" + if (basePath.isAbsolute) basePath.toUri.toString
	lazy val wikipediaDumpFilesPath = if (basePath.isAbsolute)
			basePath.toUri.toString
		else
			basePath.makeQualified(new LocalFileSystem).toUri.toString

	var environment: ExecutionEnvironment = null
	var params: Params = null

	val arguments: Seq[T]

	def buildProgram(env: ExecutionEnvironment, param: T): Unit

	def makeProgram(env: ExecutionEnvironment, params: Params): Unit = {
		makeProgram(env, params, null.asInstanceOf[T])
	}
	def makeProgram(env: ExecutionEnvironment, params: Params, param: T): Unit = {
		environment = env
		this.params = params
		buildProgram(env, param)
	}

	private lazy val wikiInput: DataSet[WikipediaPage] = environment
			.readHadoopFile( new WikipediaPageInputFormat, classOf[LongWritable], classOf[WikipediaPage], wikipediaDumpFilesPath )
			.map(_._2)
	private def readRawWikiPages[S : TypeInformation : ClassTag](fun: Extractor => S, pageFilter: String => Boolean = _ => true): DataSet[S] = {
		wikiInput
			// TODO should be map instead of flatMap, but not (yet) possible due to xmlToWikiPages signature
			.flatMap( rawPage  => new WikiPageReader().xmlToWikiPages( new StringReader(rawPage.getRawXML), pageFilter ) )
			.filter ( wikiPage => wikiPage.ns == 0 && wikiPage.source.nonEmpty )
			.flatMap( (wikiPage: WikiPage, out: Collector[S]) =>
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
			)
			.name("Raw-Wiki-Pages")
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

			FullInfoWikiPage(wikiPage.pageTitle, wikiPage.ns, wikiPage.redirect,
				tokenizerResult.getTokens, tokenizerResult.getTags, tokenizerResult.getLinkPositions, wikiPage.isDisambiguation, wikiPage.isList)
		}, pageFilter)
	}


	def filterNormalPages(wikiPages: DataSet[WikiPage]): DataSet[WikiPage] = {
		wikiPages.filter { wikiPage =>
			wikiPage.isNormalPage
		}.name("Filter-Normal-Pages")
	}

	def readPlainTexts: DataSet[PlainText] = {
		environment.readTextFile(plainTextsPath).name("Plain-Texts").flatMap { line =>
			val split = line.split('\t')
			if (split.length == 3)
				Some(PlainText(split(0), split(1), split(2)))
			else {
				log.warn(s"Line does not follow plain text standard: $line")
				None
			}
		}.name("Parsed Plain-Texts")
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
				if (split.length == 5)
					out.collect((split(0), split(3).toFloat))
				else {
					log.warn(s"SurfaceLinkProbs: Discarding '${split.deep}' because split size not correct")
					log.warn(line)
				}

			}
		}).name("Parsed Surfaces with Probabilities")
	}

	def readSurfaceDocumentCounts(): DataSet[SurfaceAsLinkCount] = {
		environment.readTextFile(surfaceDocumentCountsHalfsPath).name("Raw-Surface-Document-Counts").flatMap { line =>
			val split = line.split('\t')
			// not clear, why lines without a count occur, but they do
			try {
				if (split.length != 3) {
					log.warn(s"SurfaceDocumentCounts: Discarding '${split.deep}' because split size not correct")
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


	def readSurfaces(subFile: String = ""): DataSet[String] = {
		environment.readTextFile(surfaceDocumentCountsHalfsPath + subFile).name("Subset of Surfaces")
			.flatMap(new RichFlatMapFunction[String, String] {
			override def flatMap(line: String, out: Collector[String]): Unit = {
				val split = line.split('\t')
				if (split.length == 3)
					out.collect(split(0))
				else {
					log.warn(s"SurfaceProbs: Discarding '${split.deep}' because split size not correct")
					log.warn(line)
				}
			}
		}).name("Parsed Surfaces")
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
						Some(wordSplit(0), wordSplit(1).toInt)
					else
						None
				}.toMap
				Some(LanguageModel(pageTitle, model))
			}
		}.name("Reading language models")
	}

	def runsOffline(): Boolean = {
		CoheelProgram.runsOffline()
	}
}

abstract class NoParamCoheelProgram extends CoheelProgram[Void] {
	val arguments: Seq[Void] = List(null)

	def buildProgram(env: ExecutionEnvironment): Unit
	override def buildProgram(env: ExecutionEnvironment, param: Void): Unit = {
		buildProgram(env)
	}
}
