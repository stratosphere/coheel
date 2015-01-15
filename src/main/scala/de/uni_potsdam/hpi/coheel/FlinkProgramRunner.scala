package de.uni_potsdam.hpi.coheel

import java.io.{InputStream, InputStreamReader, File}
import java.net.InetSocketAddress

import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import org.apache.commons.collections4.trie.PatriciaTrie
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.ProgramDescription
import org.apache.flink.api.scala._
import org.apache.flink.client.program.ProgramInvocationException
import org.apache.flink.configuration.{ConfigConstants, GlobalConfiguration}
import org.apache.flink.runtime.fs.hdfs.DistributedFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import com.typesafe.config.{Config, ConfigFactory}
import de.uni_potsdam.hpi.coheel.programs._
import scala.collection.JavaConverters._

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
	val programs = Map(
		"main" -> classOf[WikipediaTrainingProgram]
		, "classify" -> classOf[ClassificationProgram]
		, "trie" -> classOf[EntireTextSurfacesProgram]
		, "ner-roc" -> classOf[NerRocCurveProgram]
		, "redirects" -> classOf[RedirectResolvingProgram]
		, "large" -> classOf[LargeFileTestProgram]
	)

	/**
	 * Command line parameter configuration
	 */
	case class Params(dataSetConf: String = "chunk",
	                  programName: String = "main",
	                  doLogging: Boolean  = false,
	                  parallelism: Int    = 10,
	                  programParams: Map[String, String] = Map()
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
				failure("program must be one of the following: " + programs.keys.mkString(",")) }
		opt[Unit]('l', "logging") action { case (_, c) =>
			c.copy(doLogging = true) }
		opt[Int]('p', "parallelism") action { (x, c) =>
			c.copy(parallelism = x) } text "specifies the degree of parallelism for Flink"
		opt[Unit]("X" + ProgramParams.ONLY_WIKIPAGES) action { (x, c) =>
			c.copy(programParams = c.programParams + (ProgramParams.ONLY_WIKIPAGES -> "true")) }
		opt[Unit]("X" + ProgramParams.ONLY_PLAINTEXTS) action { (x, c) =>
			c.copy(programParams = c.programParams + (ProgramParams.ONLY_PLAINTEXTS -> "true")) }
		help("help") text "prints this usage text"
	}

	// Configuration for various input and output folders in src/main/resources.
	var config: Config = _

	def main(args: Array[String]): Unit = {
		// Parse the arguments
		parser.parse(args, Params()) map { params =>
			config = ConfigFactory.load(params.dataSetConf)
			val programName = params.programName
			val program = programs(programName).newInstance()
			program.params = params.programParams
			runProgram(program, params.parallelism)
		} getOrElse {
			parser.showUsage
		}
	}

	def runProgram(program: CoheelProgram with ProgramDescription, parallelism: Int): Unit = {
		log.info(StringUtils.repeat('#', 140))
		log.info("# " + StringUtils.center(program.getDescription, 136) + " #")
		log.info("# " + StringUtils.rightPad(s"Dataset: ${config.getString("name")}", 136) + " #")
		log.info("# " + StringUtils.rightPad(s"Base Path: ${config.getString("base_path")}", 136) + " #")
		log.info("# " + StringUtils.rightPad(s"Output Folder: ${config.getString("output_files_dir")}", 136) + " #")
		log.info("# " + StringUtils.rightPad(s"Free Memory: ${FreeMemory.get(true)} MB", 136) + " #")
		log.info("# " + StringUtils.rightPad(s"Program Params: ${program.params}", 136) + " #")


		time {
			val env = if (config.getString("type") == "file") {
				GlobalConfiguration.loadConfiguration("conf")
				ExecutionEnvironment.createLocalEnvironment(1)
			}
			else
				ExecutionEnvironment.createRemoteEnvironment("tenemhead2", 6123, parallelism,
					"target/coheel_stratosphere-0.1-SNAPSHOT-jar-with-dependencies.jar")
			program.buildProgram(env)
			log.info("# " + StringUtils.rightPad(s"Degree of parallelism: ${env.getDegreeOfParallelism}", 136) + " #")
			log.info(StringUtils.repeat('#', 140))

			log.info("Starting ..")
//			FileUtils.writeStringToFile(new File("PLAN"), env.getExecutionPlan())
			try {
				val params = if (program.params.size > 0) " " + program.params.toString().replace("Map(", "Params(") else ""
				env.execute(s"${program.getDescription} (dataset = ${config.getString("name")}$params)")
			} catch {
				case e: ProgramInvocationException =>
					if (e.getMessage.contains("canceled"))
						println("Stopping .. Program has been canceled.")
			}
//			PerformanceTimer.printTimerEvents()
		}
	}
	def time[R](block: => R): Double = {
		val start = System.nanoTime()
		val result = block
		val end = System.nanoTime()
		val time = (end - start) / 1000 / 1000 / 1000
		log.info("Took " + time + " s.")
		time
	}
}
