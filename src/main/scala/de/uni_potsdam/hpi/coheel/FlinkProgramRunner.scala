package de.uni_potsdam.hpi.coheel

import java.io.File

import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.util.Timer
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.ProgramDescription
import org.apache.flink.api.scala._
import org.apache.flink.client.program.ProgramInvocationException
import org.apache.flink.configuration.GlobalConfiguration
import org.apache.log4j.Logger
import com.typesafe.config.{Config, ConfigFactory}
import de.uni_potsdam.hpi.coheel.programs._
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap

/**
 * Command line parameter configuration
 */
case class Params(dataSetConf: String = "chunk",
                  programName: String = "main",
                  doLogging: Boolean  = false,
                  parallelism: Int    = 10,
                  configurationParams: Map[String, String] = Map()
)

/**
 * Basic runner for several Flink programs.
 * Can be configured via several command line arguments.
 * Run without arguments for a list of available options.
 */
// GC parameters: -verbose:gc -XX:+PrintGCTimeStamps -XX:+PrintGCDetails
// Dump downloaded from http://dumps.wikimedia.org/enwiki/latest/

object FlinkProgramRunner {

	val log = Logger.getLogger(getClass)

	/**
	 * Runnable Flink programs.
	 */
	val programs = ListMap(
		"extract-main" -> classOf[WikipediaTrainingProgram]
		, "entire-text-surfaces" -> classOf[EntireTextSurfacesProgram]
		, "training-data" -> classOf[TrainingDataProgram]
		, "surface-evaluation" -> classOf[SurfaceEvaluationProgram]
		, "classification" -> classOf[ClassificationProgram]
		, "redirects" -> classOf[RedirectResolvingProgram]
		, "page-rank" -> classOf[PageRankProgram]
		, "best-practices" -> classOf[BestPracticesProgram]
	)

	val parser = new scopt.OptionParser[Params]("bin/run") {
		head("CohEEL", "0.0.1")
		opt[String]('d', "dataset") required() action { (x, c) =>
			c.copy(dataSetConf = x) } text "specifies the dataset to use, either 'full' or 'chunk'" validate { x =>
			if (List("full", "chunk", "chunk_cluster", "full_cluster").contains(x)) success
			else failure("dataset must be either 'full', 'chunk' or 'chunk_cluster', 'full_cluster'") }
		opt[String]('p', "program") required() action { (x, c) =>
			c.copy(programName = x) } text "specifies the program to run" validate { x =>
			if (programs.contains(x))
				success
			else
				failure("program must be one of the following: " + programs.keys.mkString(", ")) }
		opt[Unit]('l', "logging") action { case (_, c) =>
			c.copy(doLogging = true) }
		opt[Int]('p', "parallelism") action { (x, c) =>
			c.copy(parallelism = x) } text "specifies the degree of parallelism for Flink"
		note("Parameters starting with X denote special parameters for certain programs:")
		opt[Unit]("X" + ConfigurationParams.ONLY_WIKIPAGES) text "Only run wiki page extraction" action { (x, c) =>
			c.copy(configurationParams = c.configurationParams + (ConfigurationParams.ONLY_WIKIPAGES -> "true")) }
		help("help") text "prints this usage text"
	}

	// Configuration for various input and output folders in src/main/resources.
	var config: Config = _
	var params: Params = _

	def main(args: Array[String]): Unit = {
		// Parse the arguments
		parser.parse(args, Params()) map { params =>
			this.params = params
			config = ConfigFactory.load(params.dataSetConf)
			val programName = params.programName
			val program = programs(programName).newInstance()
			program.configurationParams = params.configurationParams
			runProgram(program, params)
		} getOrElse {
			parser.showUsage
		}
	}

	def runProgram[T](program: CoheelProgram[T] with ProgramDescription, params: Params): Unit = {
		log.info(StringUtils.repeat('#', 140))
		log.info("# " + StringUtils.center(program.getDescription, 136) + " #")
		log.info("# " + StringUtils.rightPad(s"Dataset: ${config.getString("name")}", 136) + " #")
		log.info("# " + StringUtils.rightPad(s"Base Path: ${config.getString("base_path")}", 136) + " #")
		log.info("# " + StringUtils.rightPad(s"Output Folder: ${config.getString("output_files_dir")}", 136) + " #")
		log.info("# " + StringUtils.rightPad(s"Free Memory: ${FreeMemory.get(true)} MB", 136) + " #")
		log.info("# " + StringUtils.rightPad(s"Configuration Params: ${program.configurationParams}", 136) + " #")

		val runtime = Timer.timeFunction {
			val env = if (config.getString("type") == "file") {
				GlobalConfiguration.loadConfiguration("conf")
				ExecutionEnvironment.createLocalEnvironment(1)
			}
			else
				ExecutionEnvironment.createRemoteEnvironment("tenemhead2", 6123, params.parallelism,
					"target/coheel_stratosphere-0.1-SNAPSHOT-jar-with-dependencies.jar")
			log.info("# " + StringUtils.rightPad(s"Degree of parallelism: ${env.getParallelism}", 136) + " #")
			log.info(StringUtils.repeat('#', 140))

			log.info("Starting ..")
			try {
				program.arguments.foreach { argument =>
					if (argument != null)
						log.info(s"Current parameter: $argument")
					program.makeProgram(env, params, argument)
					FileUtils.writeStringToFile(new File("PLAN"), env.getExecutionPlan())
					val configurationString = if (program.configurationParams.size > 0)
						" " + program.configurationParams.toString().replace("Map(", "configuration-params = (")
					else
						""
					val paramsString = if (argument == null) "" else s" current-param = $argument"
					FileUtils.write(new File("PLAN"), env.getExecutionPlan())
					env.getConfig.disableSysoutLogging()
					val result = env.execute(s"${program.getDescription} (dataset = ${config.getString("name")}$paramsString$configurationString)")
					val accResults = result.getAllAccumulatorResults.asScala
					accResults.foreach { case (acc, obj) =>
						println(acc)
					}
					log.info(s"Net runtime: ${result.getNetRuntime / 1000} s")
					Timer.printAll()
				}
			} catch {
				case e: ProgramInvocationException =>
					if (e.getMessage.contains("canceled"))
						println("Stopping .. Program has been canceled.")
			}
		}
		println(s"Took ${runtime / 1000} s.")
	}
}
