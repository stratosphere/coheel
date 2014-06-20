package de.hpi.uni_potsdam.coheel_stratosphere

import de.hpi.uni_potsdam.coheel_stratosphere.wiki.{WikiPageReader, LinkExtractor}
import eu.stratosphere.client.LocalExecutor
import org.apache.log4j.{Level, Logger}

object Main {

	/**
	 * Helpful commands:
	 * grep -A 5 -i "{{disambiguation" --color=always enwiki-latest-pages-articles1.xml-p000000010p000010000 | less -R
	 * Open tasks:
	 * <ul>
	 *   <li> Display plan using stratosphere's builtin visualizer
	 *   <li> Remove wiktionary links?
	 *   <li> Compact language model (use trie?)
	 */
	def main(args: Array[String]): Unit = {
		turnOffLogging()

//		val elem = XML.loadFile("src/test/resources/enwiki-latest-pages-articles1.xml-p000000010p000010000")
//		println(WikiPageReader.xmlToWikiPages(elem).next())

		// http://dumps.wikimedia.org/enwiki/latest/

		println("Parsing wikipedia.")
		time {
			val task = new WikipediaTrainingTask("src/test/resources/chunk_dump.txt")
			LocalExecutor.setOverwriteFilesByDefault(true)
			LocalExecutor.execute(task)
		}
	}

	def time[R](block: => R): R = {
		val start = System.nanoTime()
		val result = block
		val end = System.nanoTime()
		val time = (end - start) / 1000 / 1000 / 1000
		println("Took " + time + " s.")
		result
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
