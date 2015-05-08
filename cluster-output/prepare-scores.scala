//#!/opt/scala-2.11.5/bin/scala

import java.io.{File, PrintWriter}

import scala.io.Source

val reader = Source.fromFile(new File("scores.wiki"))
val writer = new PrintWriter(new File("raw-scores.tsv"))

writer.println(Array("id", "prom", "promRank", "promDeltaTop", "promDeltaSucc",
	"context", "contextRank", "contextDeltaTop", "contextDeltaSucc", "class").mkString("\t"))

reader.getLines().foreach { line =>
	val values = line.split('\t').toSeq
	assert(values.length == 13)
	val ID_INDEX = 0
	val SOURCE_INDEX = 2
	val id = (values(ID_INDEX) + values(SOURCE_INDEX)).hashCode

	val newValues = id.toString +: values.slice(4, 12) :+ (if (values.last == "true") "1.0" else "0.0")
	val newLine = newValues.mkString("\t")
	writer.println(newLine)
}
reader.close()
writer.close()
