package de.uni_potsdam.hpi.coheel.debugging

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
}
