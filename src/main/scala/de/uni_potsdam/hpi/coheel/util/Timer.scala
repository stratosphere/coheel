package de.uni_potsdam.hpi.coheel.util

import scala.collection.mutable

object Timer {

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
