package de.hpi.uni_potsdam.coheel_stratosphere

import de.hpi.uni_potsdam.coheel_stratosphere.wiki.{WikiPageReader, LinkExtractor}
import eu.stratosphere.client.LocalExecutor
import org.apache.log4j.{Level, Logger}

object Main {

	val PRODUCTION = false
	val taskFile = if (PRODUCTION)  "src/test/resources/full_dump.wikirun" else "src/test/resources/chunk_dump.wikirun"
	/**
	 * HELPFUL COMMANDS:
	 * GREP -A 5 -I "{{DISAMBIGUATION" --COLOR=ALWAYS ENWIKI-LATEST-PAGES-ARTICLES1.XML-P000000010P000010000 | LESS -R
	 * OPEN TASKS:
	 * <UL>
	 *   <LI> DISPLAY PLAN USING STRATOSPHERE'S BUILTIN VISUALIZER
	 *   <LI> REMOVE WIKTIONARY LINKS?
	 *   <LI> COMPACT LANGUAGE MODEL (USE TRIE?)
	 */
	def main(args: Array[String]): Unit = {
		turnOffLogging()
		println("Parsing wikipedia.")
		val processingTime = time {
			// Dump downloaded from http://dumps.wikimedia.org/enwiki/latest/
			val task = new WikipediaTrainingTask(taskFile)
			LocalExecutor.setOverwriteFilesByDefault(true)
			LocalExecutor.execute(task)
		} * 10.2 * 1024 /* full data dump size*/ / 42.7 /* test dump size */ / 60 /* in minutes */ / 60 /* in hours */
		if (!PRODUCTION)
			println(f"Approximately $processingTime%.2f hours on the full dump, one machine.")

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
