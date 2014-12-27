package de.uni_potsdam.hpi.coheel.debugging

import java.lang.instrument.Instrumentation

object ObjectSizeInstrumentation {
	private var instrumentation: Instrumentation = _

	def premain(args: String, inst: Instrumentation): Unit = {
		instrumentation = inst
	}

	def getObjectSize(o: Object): Long = {
		instrumentation.getObjectSize(o)
	}

}
