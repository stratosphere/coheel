package de.uni_potsdam.hpi.coheel

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.{ProgramDescription, Program}
import org.apache.flink.client.LocalExecutor
import org.apache.log4j.{Level, Logger}
import com.typesafe.config.{Config, ConfigFactory}
import de.uni_potsdam.hpi.coheel.programs.{EntireTextSurfacesProgram, RedirectResolvingProgram, WikipediaTrainingProgram}
import org.slf4s.Logging
import scala.collection.JavaConversions._

/**
 * Basic runner for several Flink programs.
 * Can be configured via several command line arguments.
 * Run without arguments for a list of available options.
 */
// -verbose:gc -XX:+PrintGCTimeStamps -XX:+PrintGCDetails
object FlinkProgramRunner extends Logging {

	/**
	 * Command line parameter configuration
	 */
	case class Params(dataSetConf: String = "chunk", programName: String = "main", doLogging: Boolean = false)
	val parser = new scopt.OptionParser[Params]("bin/run") {
		head("CohEEL", "0.0.1")
		opt[String]('d', "dataset") required() action { (x, c) =>
			c.copy(dataSetConf = x) } text "specifies the dataset to use, either 'full' or 'chunk'" validate { x =>
			if (List("full", "chunk").contains(x)) success else failure("dataset must be either 'full' or 'chunk'") }
		opt[String]('p', "program") required() action { (x, c) =>
			c.copy(programName = x) } text "specifies the program to run"
		opt[Boolean]('l', "logging") action { case (x, c) =>
			c.copy(doLogging = x) }
		note("some notes.\n")
		help("help") text "prints this usage text"
	}

	/**
	 * Runnable Flink programs.
	 */
	val programs = Map(
		"main" -> classOf[WikipediaTrainingProgram],
		"trie" -> classOf[EntireTextSurfacesProgram],
		"redirects" -> classOf[RedirectResolvingProgram])

	// Always overwrite already existing files.
	LocalExecutor.setOverwriteFilesByDefault(true)

	var config: Config = _

	def main(args: Array[String]): Unit = {
		// parser.parse returns Option[C]
		parser.parse(args, Params()) map { params =>
			config   = ConfigFactory.load(params.dataSetConf)
			val programName = params.programName
			if (!params.doLogging)
				turnOffLogging()

			val program = programs(programName).newInstance()
			runProgram(program)
		} getOrElse {
			parser.showUsage
		}
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
