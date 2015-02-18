package de.uni_potsdam.hpi.coheel.programs

import org.apache.flink.api.common.ProgramDescription
import org.apache.flink.api.scala.ExecutionEnvironment

abstract class CoheelProgram[T]() extends ProgramDescription {

	val params: Seq[T]
	var configurationParams: Map[String, String] = _
	def buildProgram(env: ExecutionEnvironment, param: T): Unit
}

abstract class NoParamCoheelProgram extends CoheelProgram[Void] {
	val params: Seq[Void] = List(null)

	def buildProgram(env: ExecutionEnvironment): Unit
	override def buildProgram(env: ExecutionEnvironment, param: Void): Unit = {
		buildProgram(env)
	}
}
