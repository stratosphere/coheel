package de.uni_potsdam.hpi.coheel.debugging

object ObjectSize {

	def main(args: Array[String]): Unit = {
		determineMemorySizeUsingInstrumentation()
	}

	private def determineMemorySizeUsingInstrumentation(): Unit = {
		// One can also determine the object size by instrumentation.
		// However, as this is more complicated (need to export a jar etc.)
		// and the method used above also provides good results this is
		// not used anymore.
		val a1 = new Array[Long](100)
		val a2 = new Array[Long](1000)
		val a3 = new Array[Byte](1000)
		println(ObjectSizeInstrumentation.getObjectSize(a1))
		println(ObjectSizeInstrumentation.getObjectSize(a2))
		println(ObjectSizeInstrumentation.getObjectSize(a3))
	}
}
