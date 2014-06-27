package de.uni_potsdam.hpi.coheel

import eu.stratosphere.client.LocalExecutor
import org.apache.log4j.{Level, Logger}
import de.uni_potsdam.hpi.coheel.wiki.TextAnalyzer
import de.uni_potsdam.hpi.coheel.plans.{SurfaceNotALinkCountPlan, WikipediaTrainingPlan}

object Main {

	val PRODUCTION = false
	val taskFile = if (PRODUCTION)  "src/test/resources/full_dump.wikirun" else "src/test/resources/chunk_dump.wikirun"
	/**
	 * Helpful commands:
	 * grep -A 5 -i "{{disambiguation" --color=always enwiki-latest-pages-articles1.xml-p000000010p000010000 | less -R
	 * Open tasks:
	 * <ul>
	 *   <li> Display plan using stratosphere's builtin visualizer
	 *   <li> Remove wiktionary links?
	 *   <li> Compact language model (use trie?)
	 */
	turnOffLogging()
	LocalExecutor.setOverwriteFilesByDefault(true)

	def main(args: Array[String]): Unit = {
//		runWikipediaTrainingPlan()
		runSurfaceNotALinkCountPlan()
	}

	def runWikipediaTrainingPlan(): Unit = {
		println("Parsing wikipedia.")
		val processingTime = time {
			// Dump downloaded from http://dumps.wikimedia.org/enwiki/latest/
			val plan = new WikipediaTrainingPlan(taskFile)
			LocalExecutor.execute(plan)
		} * 10.2 * 1024 /* full data dump size*/ / 42.7 /* test dump size */ / 60 /* in minutes */ / 60 /* in hours */
		if (!PRODUCTION)
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
