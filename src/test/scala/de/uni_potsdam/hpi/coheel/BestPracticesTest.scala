package de.uni_potsdam.hpi.coheel

import de.uni_potsdam.hpi.coheel.programs.BestPracticesProgram
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.test.util.MultipleProgramsTestBase
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import de.uni_potsdam.hpi.coheel.io.OutputFiles._

@RunWith(classOf[Parameterized])
class BestPracticesTest(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

	@Test
	def testFoo(): Unit = {
		val env = ExecutionEnvironment.getExecutionEnvironment
		val program = new BestPracticesProgram
		program.makeProgram(env)
		env.execute("Test")

		compareResultsByLinesInMemory("b", bestPracticesPath)

	}

}
