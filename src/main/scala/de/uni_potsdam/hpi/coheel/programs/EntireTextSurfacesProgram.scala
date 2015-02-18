package de.uni_potsdam.hpi.coheel.programs

import java.lang.Iterable
import java.util.Date

import de.uni_potsdam.hpi.coheel.FlinkProgramRunner
import de.uni_potsdam.hpi.coheel.datastructures.{NewTrie, Trie, HashTrie}
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.programs.DataClasses.{EntireTextSurfaces, SurfaceAsLinkCount, EntireTextSurfaceCounts}
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper
import org.apache.flink.api.common.functions.{BroadcastVariableInitializer, RichFlatMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.apache.log4j.Logger
import scala.collection.JavaConverters._

import scala.collection.mutable

object EntireTextSurfacesProgram {
	val BROADCAST_SURFACES = "surfaces"
}
class EntireTextSurfacesProgram extends CoheelProgram[Int] {


	@transient val log = Logger.getLogger(getClass)
	lazy val fileType = FlinkProgramRunner.config.getString("type")
	private val subSurfaceFile =  if (fileType == "file") "" else "/12"
	val params = if (fileType == "file") List(1) else 1 to 10
	override def getDescription = "Wikipedia Extraction: Entire Text Surfaces"

	override def buildProgram(env: ExecutionEnvironment, param: Int): Unit = {
		val plainTexts = env.readTextFile(plainTextsPath).name("Plain-Texts").flatMap { line =>
			val split = line.split('\t')
			if (split.size == 2)
				Some((split(0), split(1)))
			else
				None
		}.name("Parsed Plain-Texts")

		val surfaces = env.readTextFile(surfaceProbsPath + s"/$param").name("Subset of Surfaces")
			.flatMap(new RichFlatMapFunction[String, String] {
			override def open(params: Configuration): Unit = {
				println(s"MEMORY: ${FreeMemory.get(true)} MB")
			}
			override def flatMap(line: String, out: Collector[String]): Unit = {
				val split = line.split('\t')
				if (split.size == 3)
					out.collect(split(0))
			}
		}).name("Parsed Surfaces")

		val entireTextSurfaces = plainTexts
			.flatMap(new FindEntireTextSurfacesFlatMap)
			.withBroadcastSet(surfaces, EntireTextSurfacesProgram.BROADCAST_SURFACES)
			.name("Entire-Text-Surfaces-Along-With-Document")

		val surfaceDocumentCounts = env.readTextFile(surfaceDocumentCountsPath).name("Raw-Surface-Document-Counts")

		val entireTextSurfaceCounts = entireTextSurfaces
			.groupBy { _.surface }
			.reduceGroup { group =>
				val surfaces = group.toList
				EntireTextSurfaceCounts(surfaces.head.surface, surfaces.size)
			}
			.name("Entire-Text-Surface-Counts")

		val surfaceLinkProbs = surfaceDocumentCounts.map { line =>
			val split = line.split('\t')
			// not clear, why lines without a count occur, but they do
			try {
				if (split.size < 2)
					SurfaceAsLinkCount(split(0), 0)
				else {
					val (surface, count) = (split(0), split(1).toInt)
					SurfaceAsLinkCount(TokenizerHelper.tokenize(surface).mkString(" "), count)
				}
			} catch {
				case e: NumberFormatException =>
					SurfaceAsLinkCount(split(0), 0)
			}
		}.name("Surface-Document-Counts").join(entireTextSurfaceCounts)
			.where { _.surface }
			.equalTo { _.surface }
			.map { joinResult => joinResult match {
				case (surfaceAsLinkCount, entireTextSurfaceCount) =>
					(surfaceAsLinkCount.surface, entireTextSurfaceCount.count,
						surfaceAsLinkCount.count.toDouble / entireTextSurfaceCount.count.toDouble)
			}
		}.name("Surface-Link-Probs")

		entireTextSurfaces.writeAsTsv(entireTextSurfacesPath + s"/$param-it")
		surfaceLinkProbs.writeAsTsv(surfaceLinkProbsPath + s"/$param-it")
	}
}
class FindEntireTextSurfacesFlatMap extends RichFlatMapFunction[(String, String), EntireTextSurfaces] {
	var trie: Trie = _
	var last1000 = new Date()

	class TrieBroadcastInitializer extends BroadcastVariableInitializer[String, Trie] {

		override def initializeBroadcastVariable(surfaces: Iterable[String]): Trie = {
			val trieFromBroadcast = new NewTrie
			surfaces.asScala.foreach { surface =>
				trieFromBroadcast.add(surface)
			}
			trieFromBroadcast
		}
	}

	var i = 0
	override def open(params: Configuration): Unit = {
		println(s"Free memory, before: ${FreeMemory.get(true)} MB")
		val d1 = new Date
		trie = getRuntimeContext.getBroadcastVariableWithInitializer(EntireTextSurfacesProgram.BROADCAST_SURFACES, new TrieBroadcastInitializer)
		println(s"Trie initialization took ${(new Date().getTime - d1.getTime) / 1000} s.")
		println(s"Free memory, after: ${FreeMemory.get(true)} MB")
	}
	override def flatMap(plainText: (String, String), out: Collector[EntireTextSurfaces]): Unit = {
		if (i % 1000 == 0) {
			val new1000 = new Date()
			val difference = new1000.getTime - last1000.getTime
			println(s"${new Date()}: ENTIRETEXTSURFACES: $i, LAST 1000: $difference ms, FREE MEMORY: ${FreeMemory.get()} MB")
			last1000 = new1000
		}
		i += 1
		findEntireTextSurfaces(plainText, trie).foreach(out.collect)
	}

	def findEntireTextSurfaces(plainText: (String, String), trie: Trie): Iterator[EntireTextSurfaces] = {
		val text = plainText._2
		trie.findAllIn(text).toIterator.map { surface => EntireTextSurfaces(plainText._1, surface)}
	}
}
