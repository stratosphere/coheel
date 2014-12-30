package de.uni_potsdam.hpi.coheel.debugging

import de.uni_potsdam.hpi.coheel.datastructures.TrieBuilder

object ObjectSize {

	def main(args: Array[String]): Unit = {
//		val a1 = new Array[Long](100)
//		val a2 = new Array[Long](1000)
//		val a3 = new Array[Byte](1000)
//		println(ObjectSizeInstrumentation.getObjectSize(a1))
//		println(ObjectSizeInstrumentation.getObjectSize(a2))
//		println(ObjectSizeInstrumentation.getObjectSize(a3))


		for (i <- 1 to 10)
			System.gc()
		val before1 = FreeMemory.get()
		TrieBuilder.buildFullTrie()
//		println(MemoryMeasurer.measureBytes(TrieBuilder.fullTrie) / 1024 / 1024)
		for (i <- 1 to 10)
			System.gc()
		val after = FreeMemory.get()
		TrieBuilder.fullTrie = null
		for (i <- 1 to 10)
			System.gc()
		val before2 = FreeMemory.get()

		println(before1 - after)
		println(before2 - after)
	}
}
