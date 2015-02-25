package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.FlinkProgramRunner
import org.apache.flink.api.common.ProgramDescription
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.log4j.Logger

abstract class CoheelProgram[T]() extends ProgramDescription {

	val params: Seq[T]
	var configurationParams: Map[String, String] = _
	def buildProgram(env: ExecutionEnvironment, param: T): Unit

	@transient val log = Logger.getLogger(getClass)

	def runsOffline(): Boolean = {
		val fileType = FlinkProgramRunner.config.getString("type")
		fileType == "file"
	}
}

abstract class NoParamCoheelProgram extends CoheelProgram[Void] {
	val params: Seq[Void] = List(null)

	def buildProgram(env: ExecutionEnvironment): Unit
	override def buildProgram(env: ExecutionEnvironment, param: Void): Unit = {
		buildProgram(env)
	}
}
