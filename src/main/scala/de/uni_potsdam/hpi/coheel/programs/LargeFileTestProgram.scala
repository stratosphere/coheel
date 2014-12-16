package de.uni_potsdam.hpi.coheel.programs

import org.apache.flink.api.common.ProgramDescription
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.shaded.com.google.common.io.FileWriteMode

class LargeFileTestProgram extends CoheelProgram {

	override def getDescription = "Large file test"

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val input = env.readTextFile("hdfs://tenemhead2/home/stefan.bunk/large_file")

		val result = input.map { linesIt =>
			println(s"READNOWHERE: Reading ${linesIt.size} files.")
			(1, 1)
		}.sum(0)
		result.writeAsText("hdfs://tenemhead2/home/stefan.bunk/large_file_result", FileSystem.WriteMode.OVERWRITE)
	}
}
