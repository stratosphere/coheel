package de.uni_potsdam.hpi.coheel

import scala.collection.immutable.ListMap

object PerformanceTimer {

	// order preserving map structure
	var timers = ListMap[String, (Long, Long)]()

	def startTime(event: String): Unit = {
		timers.get(event) match {
			case Some((start, end)) => // do not update start time
			case None =>
				timers += (event -> (getTimeInMs, 0))
		}
	}

	def endTime(event: String): Unit = {
		timers.get(event) match {
			case None =>
				throw new RuntimeException("Timer not started.")
			case Some((start, end)) =>
				if (end == 0) {
					val newEnd = getTimeInMs
					timers += (event ->(start, newEnd))
//					print(event, start, newEnd)
				}
				// else ignore, because the event already finished
		}
	}

	def print(event: String, start: Long, end: Long): Unit = {
		val duration = end - start
//		List((1000L, "us"), (1000L, "ms"), (1000L, "s"), (60L, "min")).fold(duration, "ns") { (acc, n) =>
//			if (acc._1 < n._1) {
//				()
//			}
//		}
		println(f"$event%50s took ${end - start}%10s ms.")
	}

	def printTimerEvents(): Unit = {
		timers.foreach { case (event, (start, end)) =>
			print(event, start, end)
		}
	}

	private def getTimeInMs: Long = {
		System.nanoTime() / 1000 / 1000
	}

}
