package de.uni_potsdam.hpi.coheel

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
	def end(name: String): Unit = {
		if (!ON)
			return
		val startTime = startTimes(name)
		val endTime   = time()
		val timeSoFar = timesSum(name)
		timesSum += (name -> (endTime - startTime + timeSoFar))

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
