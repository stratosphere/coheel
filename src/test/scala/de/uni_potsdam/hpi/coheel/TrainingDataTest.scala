package de.uni_potsdam.hpi.coheel

import de.uni_potsdam.hpi.coheel.programs.DataClasses.{Classifiable, TrainInfo}
import de.uni_potsdam.hpi.coheel.programs.{FeatureHelper, TrainingDataStrategies, TrainingDataGroupReduce}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConverters._

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class TrainingDataTest extends FunSuite {

	val classifiables = Iterable(
		Classifiable(s"${FeatureHelper.TRIE_HIT_MARKER}-id", "surfaceRepr", Array("context"), "candidateEntity", 1.0, 1.0, 1.0, TrainInfo("source", "destination", Array[Double]())),
		Classifiable(s"${FeatureHelper.TRIE_HIT_MARKER}-id", "surfaceRepr", Array("context"), "otherEntity", 1.0, 1.0, 1.0, TrainInfo("source", "destination", Array[Double]()))
	).asJava

	test("TrainingDataStrategies.REMOVE_ENTIRE_GROUP") {
		val p = new TrainingDataGroupReduce(TrainingDataStrategies.REMOVE_ENTIRE_GROUP)
		val c = new TestCollector[String]

		p.linkDestinationsPerEntity = mutable.Map(
			"source" -> Set("candidateEntity")
		)

		p.reduce(classifiables, c)


		assert(c.collected.size === 0)
	}
	test("TrainingDataStrategies.REMOVE_CANDIDATE_ONLY") {
		val p = new TrainingDataGroupReduce(TrainingDataStrategies.REMOVE_CANDIDATE_ONLY)
		val c = new TestCollector[String]

		p.linkDestinationsPerEntity = mutable.Map(
			"source" -> Set("candidateEntity")
		)

		p.reduce(classifiables, c)


		assert(c.collected.size === 1)
	}
}
