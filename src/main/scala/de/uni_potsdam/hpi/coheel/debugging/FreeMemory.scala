package de.uni_potsdam.hpi.coheel.debugging

import org.slf4j.Logger

object FreeMemory {

	val ON = false

	def get(garbageCollect: Boolean = false, garbageCollectNr: Int = 3): Long = {
		if (!ON)
			return 0

		if (garbageCollect)
			for (i <- 1 to garbageCollectNr) System.gc()
		val maxMem   = Runtime.getRuntime.maxMemory().toDouble / 1024 / 1024
		val freeMem  = Runtime.getRuntime.freeMemory().toDouble / 1024 / 1024
		val totalMem = Runtime.getRuntime.totalMemory().toDouble / 1024 / 1024

		val actualMem = maxMem - (totalMem - freeMem)
		actualMem.toLong
	}

	def logMemory(log: Logger, name: String): Unit = {
		if (!ON)
			return

		log.trace(s"At '$name' with ${FreeMemory.get(true)} MB of RAM")
	}
}
