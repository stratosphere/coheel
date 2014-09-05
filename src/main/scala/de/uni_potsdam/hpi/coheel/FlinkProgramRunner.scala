package de.uni_potsdam.hpi.coheel


import java.io.File

import org.apache.commons.lang3.StringUtils
import org.slf4s.Logging
import eu.stratosphere.client.LocalExecutor
import org.apache.log4j.{Level, Logger}
import com.typesafe.config.ConfigFactory
import de.uni_potsdam.hpi.coheel.programs.{SurfaceNotALinkTrieProgram, RedirectResolvingProgram, SurfaceNotALinkProgram, WikipediaTrainingProgram}
import org.apache.commons.io.FileUtils
import eu.stratosphere.api.common.{ProgramDescription, Program}

object FlinkProgramRunner extends Logging {

	val config   = ConfigFactory.load()
	LocalExecutor.setOverwriteFilesByDefault(true)

	def main(args: Array[String]): Unit = {
		// -Xms3g -Xmx7g
		// -verbose:gc -XX:+PrintGCTimeStamps -XX:+PrintGCDetails

		val programName = if (args.nonEmpty) args(0) else "main"

		if (!args.contains("-log"))
			turnOffLogging()

		val program = Map(
			"main" -> classOf[WikipediaTrainingProgram],
			"surfaces" -> classOf[SurfaceNotALinkProgram],
			"trie" -> classOf[SurfaceNotALinkTrieProgram],
			"redirects" -> classOf[RedirectResolvingProgram])(programName)
		runProgram(program.newInstance())
	}

	def runProgram(program: Program with ProgramDescription): Unit = {
		log.info(StringUtils.repeat('#', 140))
		log.info("# " + StringUtils.center(program.getDescription, 136) + " #")
		log.info("# " + StringUtils.rightPad("Dataset: " + config.getString("name"), 136) + " #")
		log.info(StringUtils.repeat('#', 140))
		val processingTime = time {
			// Dump downloaded from http://dumps.wikimedia.org/enwiki/latest/

			val json = LocalExecutor.optimizerPlanAsJSON(program.getPlan())
			FileUtils.writeStringToFile(new File("plan.json"), json, "UTF-8")

			log.info("Starting ..")
			LocalExecutor.execute(program)
		} * 10.2 * 1024 /* full data dump size*/ / 42.7 /* test dump size */ / 60 /* in minutes */ / 60 /* in hours */
		if (config.getBoolean("print_approximation"))
			log.info(f"Approximately $processingTime%.2f hours on the full dump, one machine.")
	}
	def time[R](block: => R): Double = {
		val start = System.nanoTime()
		val result = block
		val end = System.nanoTime()
		val time = (end - start) / 1000 / 1000 / 1000
		log.info("Took " + time + " s.")
		time
	}

	def turnOffLogging(): Unit = {
		List(
			classOf[eu.stratosphere.nephele.taskmanager.TaskManager],
			classOf[eu.stratosphere.nephele.execution.ExecutionStateTransition],
			classOf[eu.stratosphere.nephele.client.JobClient],
			classOf[eu.stratosphere.nephele.jobmanager.JobManager],
			classOf[eu.stratosphere.nephele.jobmanager.scheduler.AbstractScheduler],
			classOf[eu.stratosphere.nephele.instance.local.LocalInstanceManager],
			classOf[eu.stratosphere.nephele.executiongraph.ExecutionGraph],
			classOf[eu.stratosphere.compiler.PactCompiler],
			classOf[eu.stratosphere.runtime.io.network.bufferprovider.GlobalBufferPool],
			classOf[eu.stratosphere.runtime.io.network.netty.NettyConnectionManager],
			classOf[eu.stratosphere.nephele.jobmanager.splitassigner.InputSplitAssigner],
			classOf[eu.stratosphere.nephele.jobmanager.splitassigner.InputSplitManager],
			classOf[eu.stratosphere.nephele.jobmanager.splitassigner.file.FileInputSplitList],
			classOf[eu.stratosphere.nephele.jobmanager.scheduler.AbstractScheduler],
			classOf[eu.stratosphere.pact.runtime.iterative.task.IterationTailPactTask[_, _]],
			classOf[eu.stratosphere.pact.runtime.iterative.task.IterationSynchronizationSinkTask],
			classOf[eu.stratosphere.pact.runtime.iterative.task.IterationIntermediatePactTask[_, _]],
			classOf[eu.stratosphere.pact.runtime.iterative.task.IterationHeadPactTask[_, _, _, _]],
			classOf[eu.stratosphere.pact.runtime.iterative.convergence.WorksetEmptyConvergenceCriterion]
		).foreach {
			Logger.getLogger(_).setLevel(Level.WARN)
		}
	}
}
