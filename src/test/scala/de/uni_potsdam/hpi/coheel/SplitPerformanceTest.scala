package de.uni_potsdam.hpi.coheel

import java.io.File
import java.util.StringTokenizer

import org.scalatest.FunSuite

import scala.io.Source

class SplitPerformanceTest extends FunSuite {

	test("performance of the trie") {
		PerformanceTimer.startTime("READING")
		val classLoader = getClass.getClassLoader
		val plainTextFile = new File(classLoader.getResource("plain-texts").getFile)
		val content = Source.fromFile(plainTextFile).getLines().take(10000).mkString(" ")
		println(s"Setup: Read file of ${plainTextFile.length() / 1024 / 1024} MB " +
			s"in ${PerformanceTimer.endTime("READING") / 1000} s.")

		val splitMethods = List(
			("stringSplit    ", stringSplit _),
			("stringTokenizer", stringTokenizer _),
			("indexOf        ", indexOf _)
		)

		val RUNS = 10
		splitMethods.foreach { case (name, splitter) =>
			for (i <- 1 to RUNS) {
				PerformanceTimer.startTime(s"$name $i")
				val resultSize = splitter(content)
				if (i == RUNS)
					println(s"$name finished with $resultSize in ${PerformanceTimer.endTime(s"$name $i")} ms.")
			}
		}
	}

	def stringSplit(text: String): Int = {
		text.split(' ').size
	}

	def stringTokenizer(text: String): Int = {
		val tokenizer = new StringTokenizer(text)
		var num = 0
		while (tokenizer.hasMoreTokens) {
			num += 1
			tokenizer.nextToken()
		}
		num
	}

	def indexOf(text: String): Int = {
		var pos = 0
		var end = text.indexOf(' ', pos)
		var num = 0
		while (end >= 0) {
			text.substring(pos, end)
			num += 1
			pos = end + 1
			end = text.indexOf(' ', pos)
		}
		num
	}
}
