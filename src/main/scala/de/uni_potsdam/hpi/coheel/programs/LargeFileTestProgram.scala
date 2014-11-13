package de.uni_potsdam.hpi.coheel.programs

import org.apache.flink.api.common.ProgramDescription
import org.apache.flink.api.scala._

class LargeFileTestProgram extends CoheelProgram with ProgramDescription {

	override def getDescription = "Large file test"

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val input = env.readTextFile("hdfs://tenemhead2/home/stefan.bunk/large_file")

		val result = input.mapPartition { linesIt =>
			println(s"READHERE: Reading ${linesIt.size} files.")
			List((1, 1))
		}.sum(0)
		result.writeAsText("hdfs://tenemhead2/home/stefan.bunk/large_file_result")
	}
}
