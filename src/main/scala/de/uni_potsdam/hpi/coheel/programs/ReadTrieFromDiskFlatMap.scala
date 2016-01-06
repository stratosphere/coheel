package de.uni_potsdam.hpi.coheel.programs

import java.io.File
import java.util.Date

import de.uni_potsdam.hpi.coheel.Params
import de.uni_potsdam.hpi.coheel.datastructures.NewTrie
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration

import scala.io.Source

/**
  * Abstract base class for flatmaps which need to build the trie based on data on the disk
  */
abstract class ReadTrieFromDiskFlatMap[IN, OUT](params: Params) extends RichFlatMapFunction[IN, OUT] {
	import CoheelLogger._
	var trie: NewTrie = _

	override def open(conf: Configuration): Unit = {
		val surfaceLinkProbsFile = if (CoheelProgram.runsOffline()) {
			new File("output/surface-link-probs.wiki")
			//			new File("cluster-output/678910")
		} else {
			if (getRuntimeContext.getIndexOfThisSubtask < params.parallelism / 2)
				new File(params.config.getString("first_trie_half"))
			else
				new File(params.config.getString("second_trie_half"))
		}
		val surfaces = Source.fromFile(surfaceLinkProbsFile, "UTF-8").getLines().flatMap { line =>
			val split = line.split('\t')
			if (split.length == 5)
				Some(split(0), split(3).toFloat)
			else {
				log.warn(s"SurfaceLinkProbs: Discarding '${split.deep}' because split size not correct")
				log.warn(line)
				None
			}
		}
		log.info(s"On subtask id #${getRuntimeContext.getIndexOfThisSubtask} with file ${surfaceLinkProbsFile.getName}")
		log.info(s"Building trie with ${FreeMemory.get(true)} MB")
		val d1 = new Date
		trie = new NewTrie
		surfaces.foreach { case (surface, prob) =>
			// TODO: Determine heuristic for this value
			if (prob > 0.05) {
				trie.add(surface, prob)
			}
		}
		log.info(s"Finished trie with ${FreeMemory.get(true)} MB in ${(new Date().getTime - d1.getTime) / 1000} s")
	}

	override def close(): Unit = {
		trie = null
	}
}
