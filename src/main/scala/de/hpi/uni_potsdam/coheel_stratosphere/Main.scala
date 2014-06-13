package de.hpi.uni_potsdam.coheel_stratosphere

import de.hpi.uni_potsdam.coheel_stratosphere.wiki.{WikiPageReader, LinkExtractor}
import org.slf4s.Logging
import eu.stratosphere.client.LocalExecutor
import org.apache.log4j.{Level, Logger}

object Main {

	/**
	 * Open tasks:
	 * <ul>
	 *   <li> Use a trie for NER
	 *   <li> Wikipedia, handle disambiguation sites, handle list sites
	 *   <li> Compact language model
	 */
	def main(args: Array[String]): Unit = {
		turnOffLogging()

//		val elem = XML.loadFile("src/test/resources/enwiki-latest-pages-articles1.xml-p000000010p000010000")
//		println(WikiPageReader.xmlToWikiPages(elem).next())

		// http://dumps.wikimedia.org/enwiki/latest/

		val task = new WikipediaTrainingTask("src/test/resources/chunk_dump.txt")
		LocalExecutor.setOverwriteFilesByDefault(true)
		LocalExecutor.execute(task)
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
