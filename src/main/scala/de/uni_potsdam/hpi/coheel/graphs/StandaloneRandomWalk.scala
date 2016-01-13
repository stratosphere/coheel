package de.uni_potsdam.hpi.coheel.graphs

import java.io.File
import de.uni_potsdam.hpi.coheel.datastructures.TrieHit
import de.uni_potsdam.hpi.coheel.programs.DataClasses.{Neighbour, NodeTypes, ClassifierResultWithNeighbours}
import de.uni_potsdam.hpi.coheel.programs.{CoheelLogger, RandomWalkReduceGroup}
import de.uni_potsdam.hpi.coheel.util.Timer
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.collection.JavaConverters._



object StandaloneRandomWalk {

	def main(args: Array[String]): Unit = {
		val l = readOfflineFile()
		println("Done")

		val p = new RandomWalkReduceGroup()
		val c = new Collector[(String, TrieHit, String)] {
			override def collect(record: (String, TrieHit, String)): Unit = {}
			override def close(): Unit = {}
		}
		Timer.start("RW")
		p.reduce(l.toIterable.asJava, c)
		Timer.logResult(CoheelLogger.log, "RW")
	}

	def readOfflineFile(): mutable.ArrayBuffer[ClassifierResultWithNeighbours] = {
		val l = new ArrayBuffer[ClassifierResultWithNeighbours]()
		var c: ClassifierResultWithNeighbours = null
		Source.fromFile(new File("offline")).getLines().foreach { line =>
			if (line.startsWith("I,")) {
				val split = line.split(',')
				val s = split.slice(1, split.length - 1).mkString(",")
				val n = Neighbour(s, split.last.toFloat)
				c.in.asInstanceOf[ArrayBuffer[Neighbour]].+=:(n)
			} else if (line.startsWith("O,")) {
				val split = line.split(',')
				val s = split.slice(1, split.length - 1).mkString(",")
				val n = Neighbour(s, split.last.toFloat)
				c.out.asInstanceOf[ArrayBuffer[Neighbour]].+=:(n)
			} else if (line.startsWith("-------")) {
				if (c != null) {
					l += c
				}
			} else {
				val classifierType = if (line.contains(",SEED,"))
					NodeTypes.SEED
				else if (line.contains(",CANDIDATE,"))
					NodeTypes.CANDIDATE
				else
					throw new Exception(s"Unknown type in '$line'")


				var offset = line.indexOf(",SEED,")
				if (offset == -1)
					offset = line.indexOf(",CANDIDATE,")
				val candidateEntity = line.substring(0, offset)

				val trieHit = parseTrieHit(line)
				c = ClassifierResultWithNeighbours("1", classifierType, candidateEntity, trieHit, mutable.ArrayBuffer(), mutable.ArrayBuffer())
//				println(c)
			}
		}
		// append the last c
		l += c
		l
	}

	def parseTrieHit(line: String): TrieHit = {
		val R = """'TrieHit\(.*\)'""".r
		val s = R.findFirstIn(line).get
//		println(s)
		val split = s.replace("'TrieHit(", "").replace(")'", "").split(",")
//		println(split.deep)
		TrieHit(split(0), split(1).toFloat, split(2).toInt, split(3).toInt)
	}

}
