package de.uni_potsdam.hpi.coheel

import java.io.{ObjectOutputStream, FileOutputStream, File}
import java.nio.ByteBuffer

import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.io.OutputFiles
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
import weka.classifiers.meta.SerialVersionAccess
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap

/**
 * Command line parameter configuration
 */
case class Params(configName: String = "local",
                  programName: String = "extract-main",
                  parallelism: Int    = 10,
                  config: Config = null
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
	)

	val parser = new scopt.OptionParser[Params]("coheel") {
		head("CohEEL", "0.0.1")
		opt[String]('c', "configuration") required() action { (x, c) =>
			c.copy(configName = x) } text "specifies the configuration file to use, either 'local', 'cluster_tenem' or 'cluster_aws'" validate { x =>
			if (List("local", "cluster_tenem", "cluster_aws").contains(x)) success
			else failure("configuration unknown") }
		opt[String]('p', "program") required() action { (x, c) =>
			c.copy(programName = x) } text "specifies the program to run" validate { x =>
			if (programs.contains(x))
				success
			else
				failure("program must be one of the following: " + programs.keys.mkString(", ")) }
		opt[Int]("parallelism") action { (x, c) =>
			c.copy(parallelism = x) } text "specifies the degree of parallelism for Flink"
		help("help") text "prints this usage text"
	}

	// Configuration for various input and output folders in src/main/resources.
	var config: Config = _
	var params: Params = _

	def main(args: Array[String]): Unit = {
		// Parse the arguments
		parser.parse(args, Params()) map { params =>
			this.params = params
			config = ConfigFactory.load(params.configName)
			val programName = params.programName
			val program = programs(programName).newInstance()
			runProgram(program, params.copy(config = config))
		} getOrElse {
			parser.showUsage
		}
	}

	def runProgram[T](program: CoheelProgram[T] with ProgramDescription, params: Params): Unit = {
		log.info(StringUtils.repeat('#', 140))
		log.info("# " + StringUtils.center(program.getDescription, 136) + " #")
		log.info("# " + StringUtils.rightPad(s"Job Manager: ${config.getString("job_manager_host")}:${config.getString("job_manager_port")}", 136) + " #")
		log.info("# " + StringUtils.rightPad(s"Configuration: ${params.configName}", 136) + " #")
		log.info("# " + StringUtils.rightPad(s"Base Path: ${program.wikipediaDumpFilesPath}", 136) + " #")
		log.info("# " + StringUtils.rightPad(s"Output Folder: ${config.getString("output_files_dir")}", 136) + " #")
		log.info("# " + StringUtils.rightPad(s"Free Memory: ${FreeMemory.get(true)} MB", 136) + " #")

		val runtime = Timer.timeFunction {
			val env = if (config.getString("type") == "local") {
				GlobalConfiguration.loadConfiguration("conf")
				ExecutionEnvironment.createLocalEnvironment(1)
			}
			else {
				val classPath = System.getProperty("java.class.path")
				val dependencies = "target/coheel_stratosphere-0.1-SNAPSHOT.jar" :: classPath.split(':').filter(p => p.contains(".m2/")).toList
				ExecutionEnvironment.createRemoteEnvironment(config.getString("job_manager_host"), config.getInt("job_manager_port"), params.parallelism, dependencies: _*)
			}

			log.info("# " + StringUtils.rightPad(s"Degree of parallelism: ${env.getParallelism}", 136) + " #")
			log.info(StringUtils.repeat('#', 140))

			log.info("Starting ..")
			program.arguments.foreach { argument =>
				if (argument != null)
					log.info(s"Current parameter: $argument")
				program.makeProgram(env, params, argument)
				FileUtils.writeStringToFile(new File("plans/PLAN"), env.getExecutionPlan())
				val paramsString = if (argument == null) "" else s" current-param = $argument"
				FileUtils.write(new File("PLAN"), env.getExecutionPlan())
				env.getConfig.disableSysoutLogging()
				val result = env.execute(s"${program.getDescription} (configuration = ${params.configName}$paramsString)")
				val accResults = result.getAllAccumulatorResults.asScala
				accResults.foreach { case (acc, obj) =>
					println(acc)
				}
				log.info(s"Net runtime: ${result.getNetRuntime / 1000} s")
				Timer.printAll()
			}
		}
		println(s"Took ${runtime / 1000} s.")
	}
}
