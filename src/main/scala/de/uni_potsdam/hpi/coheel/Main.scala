package de.uni_potsdam.hpi.coheel

import eu.stratosphere.client.LocalExecutor
import org.apache.log4j.{Level, Logger}
import com.typesafe.config.ConfigFactory
import de.uni_potsdam.hpi.coheel.programs.{RedirectResolvingProgram, SurfaceNotALinkCountProgram, WikipediaTrainingProgram}
import org.apache.commons.io.FileUtils
import scala.collection.JavaConversions._
import java.io._
import eu.stratosphere.api.common.Program

object Main {

	val config   = ConfigFactory.load()
	turnOffLogging()
	LocalExecutor.setOverwriteFilesByDefault(true)

	def main(args: Array[String]): Unit = {
		// -Xms3g -Xmx7g
		// -verbose:gc -XX:+PrintGCTimeStamps -XX:+PrintGCDetails
		// 4379 pages in the first chunk dump

		val program = if (config.getBoolean("is_production"))
			new WikipediaTrainingProgram()
		else
			new WikipediaTrainingProgram()
//			new SurfaceNotALinkCountProgram
//		    new RedirectResolvingProgram
//		runProgram(program)
		buildRocPlot()
	}

	def runProgram(program: Program): Unit = {
		println("Parsing wikipedia. Dataset: " + config.getString("name"))
		val processingTime = time {
			// Dump downloaded from http://dumps.wikimedia.org/enwiki/latest/

			val json = LocalExecutor.optimizerPlanAsJSON(program.getPlan())
			FileUtils.writeStringToFile(new File("plan.json"), json, "UTF-8")

			LocalExecutor.execute(program)
		} * 10.2 * 1024 /* full data dump size*/ / 42.7 /* test dump size */ / 60 /* in minutes */ / 60 /* in hours */
		if (config.getBoolean("print_approximation"))
			println(f"Approximately $processingTime%.2f hours on the full dump, one machine.")
	}

	def buildRocPlot(): Unit = {
		val bw = new BufferedWriter(new FileWriter("plots/roc.csv", false))
		(0 to 4400 by 250).foreach { threshold =>
			var tp = 0
			var fp = 0
			var fn = 0
			var tn = 0

			val br = new BufferedReader(new FileReader("testoutput/possible-surface-occurrences.wiki"))
			var line: String = br.readLine()
			while (line != null) {
				val split = line.split('\t')
				val actual = split(2).toInt
				val possible = split(3).toInt
				if (possible > threshold) {
					fn += actual
					tn += (possible - actual)
				} else {
					tp += actual
					fp += (possible - actual)
				}
				line = br.readLine()
			}

			val tpr = tp.toDouble / (tp + fn).toDouble
			val fpr = fp.toDouble / (fp + tn).toDouble

			bw.write(s"$fpr,$tpr,$threshold,$tp,$tn,$fp,$fn")
			bw.newLine()
		}
		bw.close()
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
			classOf[eu.stratosphere.runtime.io.network.bufferprovider.GlobalBufferPool],
			classOf[eu.stratosphere.runtime.io.network.netty.NettyConnectionManager],
			classOf[eu.stratosphere.nephele.jobmanager.splitassigner.InputSplitAssigner],
			classOf[eu.stratosphere.nephele.jobmanager.splitassigner.InputSplitManager],
			classOf[eu.stratosphere.nephele.jobmanager.splitassigner.file.FileInputSplitList],
			classOf[eu.stratosphere.nephele.jobmanager.scheduler.AbstractScheduler]
		).foreach {
			Logger.getLogger(_).setLevel(Level.WARN)
		}
	}
}
