package de.uni_potsdam.hpi.coheel

import java.io.{FileWriter, BufferedWriter, File}

import de.uni_potsdam.hpi.coheel.datastructures.Trie
import de.uni_potsdam.hpi.coheel.programs.OutputFiles._
import de.uni_potsdam.hpi.coheel.wiki.TextAnalyzer
import org.slf4s.Logging

import scala.io.Source

object TrieTester {

	def main(args: Array[String]): Unit = {
		val trieBuilder = new TrieBuilder
//		trieBuilder.printMemoryStatus()
		trieBuilder.tokenizeSurfaces()
//		val trie = trieBuilder.buildTrie()
		trieBuilder.printMemoryStatus()
	}

}
object TrieBuilder {

	// builds the trie by accessing the lazy value
	def build(): Unit = trie

	lazy val trie: Trie = {
		new TrieBuilder().buildTrie()
	}
}
class TrieBuilder extends Logging {

	val TOKENIZED_SURFACES_FILE = s"${FlinkProgramRunner.config.getString("output_files_dir")}/tokenized-surfaces"

	def printMemoryStatus(): Unit = {
		val maxMem   = Runtime.getRuntime.maxMemory().toDouble / 1024 / 1024
		val freeMem  = Runtime.getRuntime.freeMemory().toDouble / 1024 / 1024
		val totalMem = Runtime.getRuntime.totalMemory().toDouble / 1024 / 1024
		val actualMem = maxMem - (totalMem - freeMem)
		log.info(f"Act. : $actualMem%.2f MB")
	}

	def tokenizeSurfaces(): Unit = {
		val fileName = surfaceProbsPath.replace("file://", "")
		val lines = Source.fromFile(new File(fileName)).getLines()

		val bw = new BufferedWriter(new FileWriter(new File(TOKENIZED_SURFACES_FILE), false))

		var i = 0
		lines.foreach { line =>
			val surface = line.split('\t')(0)
			val tokens = TextAnalyzer.tokenize(surface)
			if (tokens.nonEmpty) {
				bw.write(tokens.mkString("\t"))
				bw.newLine()
			}
			i += 1
			if (i % 1000000 == 0) {
				log.info(f"$i")
				printMemoryStatus()
			}
		}
		bw.close()
	}
	def buildTrie(): Trie = {
		val lines = Source.fromFile(new File(TOKENIZED_SURFACES_FILE)).getLines()

		var i = 0
		val trie = new Trie()
		lines.foreach { line =>
			try {
				val tokens = line.split('\t')
				if (tokens.nonEmpty)
					trie.add(tokens)
				i += 1
				if (i % 1000000 == 0) {
//					log.info(f"$i")
//					printMemoryStatus()
				}
			} catch {
				case e: OutOfMemoryError =>
					log.error(e.toString)
					log.error(i.toString)
					System.exit(1)
			}
		}
		log.info("Built trie.")
		trie
	}

}
