package de.uni_potsdam.hpi.coheel.programs

import org.apache.flink.api.scala.ExecutionEnvironment

abstract class CoheelProgram() {
	def buildProgram(env: ExecutionEnvironment): Unit
}
