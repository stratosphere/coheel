package de.uni_potsdam.hpi.coheel

import org.apache.log4j.Logger

import scala.collection.immutable.ListMap

object PerformanceTimer {
	// order preserving map structure
	var timers = ListMap[String, (Long, Long)]()

	def startTime(event: String): Unit = startTimeFirst(event)
	def endTime(event: String): Long   = endTimeFirst(event)

	def startTimeFirst(event: String): Unit = {
		timers.get(event) match {
			case Some((start, end)) => // do not update start time
			case None =>
				timers += (event -> (getTimeInMs, 0))
		}
	}

	def startTimeLast(event: String): Unit = {
		timers += (event -> (getTimeInMs, 0))
	}

	def endTimeFirst(event: String): Long = {
		timers.get(event) match {
			case None =>
				throw new Exception("Timer not started.")
			case Some((start, end)) =>
				if (end == 0) {
					val newEnd = getTimeInMs
					timers += (event -> (start, newEnd))
					newEnd - start
				} else
					throw new Exception("Timer already finished.")
		}
	}
	def endTimeLast(event: String): Long = {
		timers.get(event) match {
			case None =>
				throw new Exception("Timer not started.")
			case Some((start, end)) =>
				val newEnd = getTimeInMs
				timers += (event ->(start, newEnd))
				newEnd - start
		}
	}

	def printTimerEvents(): Unit = {
		val maxLength = timers.map(_._1.size).max
		timers.foreach { case (event, (start, end)) =>
			println(f"${event.padTo(maxLength, ' ')} took ${end - start}%10s ms.")
		}
	}

	private def getTimeInMs: Long = {
		System.nanoTime() / 1000 / 1000
	}

}
