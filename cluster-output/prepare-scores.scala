#!/bin/sh
exec scala "$0" "$@"
!#

import java.io.{File, PrintWriter}

import scala.io.Source

if (args.length != 1) {
	println("Usage: ./prepare-scores.scala [training-data-file]")
	System.exit(1)
}

val inputFile = new File(args(0))
val input = Source.fromFile(inputFile)
val inputFolder = inputFile.getParentFile()
val outputFile = new PrintWriter(new File(inputFolder, "raw-training-data.tsv"))

outputFile.println(Array("id", "NN", "NNP", "JJ", "VB", "CD", "SYM", "W", "prom", "promRank", "promDeltaTop", "promDeltaSucc",
	"context", "contextRank", "contextDeltaTop", "contextDeltaSucc", "class").mkString("\t"))

input.getLines().foreach { line =>
	val values = line.split('\t').toSeq
	assert(values.length == 20)
	val ID_INDEX = 0
	val SOURCE_INDEX = 2
	val id = values(ID_INDEX)

	val newValues = id.toString +: values.slice(4, values.size - 1) :+ (if (values.last == "true") "1.0" else "0.0")
	val newLine = newValues.mkString("\t")
	outputFile.println(newLine)
}
input.close()
outputFile.close()

