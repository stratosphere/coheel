package de.uni_potsdam.hpi.coheel


import java.io.File

import ch.qos.logback.core.Appender
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.{ProgramDescription, Program}
import org.apache.flink.client.LocalExecutor
import org.apache.log4j.{ConsoleAppender, Level, Logger}
import com.typesafe.config.ConfigFactory
import de.uni_potsdam.hpi.coheel.programs.{WholeTextSurfacesProgram, RedirectResolvingProgram, WikipediaTrainingProgram}
import org.slf4s.Logging
import scala.collection.JavaConversions._

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
			"trie" -> classOf[WholeTextSurfacesProgram],
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
			classOf[org.apache.flink.runtime.taskmanager.TaskManager],
			classOf[org.apache.flink.runtime.execution.ExecutionStateTransition],
			classOf[org.apache.flink.runtime.client.JobClient],
			classOf[org.apache.flink.runtime.jobmanager.JobManager],
			classOf[org.apache.flink.runtime.instance.LocalInstanceManager],
			classOf[org.apache.flink.runtime.executiongraph.ExecutionGraph],
			classOf[org.apache.flink.compiler.PactCompiler],
			classOf[org.apache.flink.runtime.instance.DefaultInstanceManager],
			classOf[org.apache.flink.runtime.jobmanager.scheduler.DefaultScheduler],
			classOf[org.apache.flink.runtime.io.network.bufferprovider.GlobalBufferPool],
			classOf[org.apache.flink.runtime.io.network.netty.NettyConnectionManager],
			classOf[org.apache.flink.runtime.jobmanager.splitassigner.InputSplitAssigner],
			classOf[org.apache.flink.runtime.jobmanager.splitassigner.InputSplitManager],
			classOf[org.apache.flink.runtime.jobmanager.splitassigner.file.FileInputSplitList],
			classOf[org.apache.flink.runtime.iterative.task.IterationTailPactTask[_, _]],
			classOf[org.apache.flink.runtime.iterative.task.IterationSynchronizationSinkTask],
			classOf[org.apache.flink.runtime.iterative.task.IterationIntermediatePactTask[_, _]],
			classOf[org.apache.flink.runtime.iterative.task.IterationHeadPactTask[_, _, _, _]],
			classOf[org.apache.flink.runtime.iterative.convergence.WorksetEmptyConvergenceCriterion]
		).foreach { logClass =>
			val logger = Logger.getLogger(logClass)
			logger.setLevel(Level.WARN)
			logger.getAllAppenders.foreach { appender =>
				log.info("Appender-Class: " + appender.getClass)
			}
		}
	}
}
