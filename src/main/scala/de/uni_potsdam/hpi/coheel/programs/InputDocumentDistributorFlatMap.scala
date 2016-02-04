package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.Params
import de.uni_potsdam.hpi.coheel.programs.DataClasses.InputDocument
import de.uni_potsdam.hpi.coheel.util.Util
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.apache.log4j.Logger

import scala.util.Random

class InputDocumentDistributorFlatMap(params: Params, runsOffline: Boolean) extends RichFlatMapFunction[String, InputDocument] {

	def log: Logger = Logger.getLogger(getClass)

	var index: Int = -1
	var random: Random = null
	val parallelism = params.parallelism
	log.info(s"Basing distribution on parallelism $parallelism")
	val halfParallelism = if (CoheelProgram.runsOffline()) 1 else parallelism / 2
	val firstHalf  = if (runsOffline) List(0) else List.range(0, halfParallelism)
	val secondHalf = if (runsOffline) List(0) else List.range(halfParallelism, parallelism)
	var isFirstHalf: Boolean = true

	override def open(params: Configuration): Unit = {
		index = getRuntimeContext.getIndexOfThisSubtask
		isFirstHalf = firstHalf contains index
		random = new Random()
	}
	override def flatMap(text: String, out: Collector[InputDocument]): Unit = {
		val tokenizerResult = TokenizerHelper.tokenizeWithStemmedAndUnstemmed(text)
		val id = Util.id(text)
		log.info(s"Reading document $id on index $index")

		val tokensStemmed = tokenizerResult.tokensStemmed
		val tokensUnstemmed = tokenizerResult.tokensUnstemmed
		val tags = tokenizerResult.tags

		if (isFirstHalf) {
			out.collect(InputDocument(id, 0, index, tokensStemmed, tokensUnstemmed, tags))
			if (!CoheelProgram.runsOffline()) {
				val randomIndex = secondHalf(random.nextInt(halfParallelism))
				out.collect(InputDocument(id, 1, randomIndex, tokensStemmed, tokensUnstemmed, tags))
				log.info(s"Distributing to $index and $randomIndex")
			}
		} else {
			if (!CoheelProgram.runsOffline()) {
				val randomIndex = firstHalf(random.nextInt(halfParallelism))
				out.collect(InputDocument(id, 0, randomIndex, tokensStemmed, tokensUnstemmed, tags))
				log.info(s"Distributing to $index and $randomIndex")
			}
			out.collect(InputDocument(id, 1, index, tokensStemmed, tokensUnstemmed, tags))
		}
	}
}
