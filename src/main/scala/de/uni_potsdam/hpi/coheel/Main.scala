package de.uni_potsdam.hpi.coheel

import eu.stratosphere.client.LocalExecutor
import org.apache.log4j.{Level, Logger}
import com.typesafe.config.ConfigFactory
import de.uni_potsdam.hpi.coheel.plans.{SurfaceNotALinkCountPlan, WikipediaTrainingPlan}
import org.apache.commons.io.FileUtils
import scala.collection.JavaConversions._
import java.io.File

object Main {

	val config   = ConfigFactory.load()
	val taskFile = config.getString("base_path") + config.getString("dump_file")
	/**
	 * Helpful commands:
	 * grep -A 5 -i "{{disambiguation" --color=always enwiki-latest-pages-articles1.xml-p000000010p000010000 | less -R
	 * Open tasks:
	 * <ul>
	 *   <li> Remove wiktionary links?
	 */
	turnOffLogging()
	LocalExecutor.setOverwriteFilesByDefault(true)

	def main(args: Array[String]): Unit = {
		// -Xms3g -Xmx7g
		// -verbose:gc -XX:+PrintGCTimeStamps -XX:+PrintGCDetails
		runWikipediaTrainingPlan()
//		runSurfaceNotALinkCountPlan()
	}

	def runWikipediaTrainingPlan(): Unit = {
		println("Parsing wikipedia.")
		val processingTime = time {
			// Dump downloaded from http://dumps.wikimedia.org/enwiki/latest/
			val program = new WikipediaTrainingPlan(taskFile)

			val json = LocalExecutor.optimizerPlanAsJSON(program.getPlan())
			FileUtils.writeStringToFile(new File("plan.json"), json, "UTF-8")

			LocalExecutor.execute(program)
		} * 10.2 * 1024 /* full data dump size*/ / 42.7 /* test dump size */ / 60 /* in minutes */ / 60 /* in hours */
		if (config.getBoolean("print_approximation"))
			println(f"Approximately $processingTime%.2f hours on the full dump, one machine.")
	}

	def runSurfaceNotALinkCountPlan(): Unit = {
		val plan = new SurfaceNotALinkCountPlan
		LocalExecutor.execute(plan)
	}

	def time[R](block: => R): Double = {
		val start = System.nanoTime()
		val result = block
		val end = System.nanoTime()
		val time = (end - start) / 1000 / 1000 / 1000
		println("Took " + time + " s.")
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
			classOf[eu.stratosphere.nephele.taskmanager.bufferprovider.GlobalBufferPool],
			classOf[eu.stratosphere.nephele.taskmanager.bytebuffered.ByteBufferedChannelManager],
			classOf[eu.stratosphere.nephele.jobmanager.splitassigner.InputSplitAssigner],
			classOf[eu.stratosphere.nephele.jobmanager.splitassigner.InputSplitManager],
			classOf[eu.stratosphere.nephele.jobmanager.splitassigner.file.FileInputSplitList],
			classOf[eu.stratosphere.nephele.jobmanager.scheduler.AbstractScheduler]
		).foreach {
			Logger.getLogger(_).setLevel(Level.WARN)
		}
	}
}
