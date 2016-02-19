package de.uni_potsdam.hpi.coheel.util

import org.slf4j.Logger

import scala.collection.mutable

object Timer {

	def timeFunction[R](block: => R): Double = {
		val start = System.nanoTime()
		val result = block
		val end = System.nanoTime()
		val time = (end - start) / 1000 / 1000
		time
	}

//	def timeFunction[R](block: => R): (R, Double) = {
//		val start = System.nanoTime()
//		val result = block
//		val end = System.nanoTime()
//		val time = (end - start) / 1000 / 1000
//		(result, time)
//	}

	val ON = true

	var timesSum = mutable.LinkedHashMap[String, Long]().withDefaultValue(0L)
	var startTimes = mutable.LinkedHashMap[String, Long]()

	def start(name: String): Unit = {
		if (!ON)
			return
		startTimes += (name -> time())
	}
	def end(name: String): Long = {
		if (!ON)
			return -1
		val startTime = startTimes(name)
		val endTime   = time()
		val timeDiff = endTime - startTime
		val timeSoFar = timesSum(name)
		timesSum += (name -> (timeDiff + timeSoFar))
		timeDiff
	}

	def logResult(log: Logger, name: String): Unit = {
		log.trace(s"Method $name took ${Timer.end(name)} ms.")
	}

	def time(): Long = {
		System.currentTimeMillis()
	}

	def printAll(): Unit = {
		if (!ON)
			return
		startTimes.keys.foreach { name =>
			val timeSum = timesSum(name)
			println(f"$name%30s took $timeSum ms.")
		}
	}

	def reset(): Unit = {
		if (!ON)
			return
		timesSum = timesSum.empty
	}
}
