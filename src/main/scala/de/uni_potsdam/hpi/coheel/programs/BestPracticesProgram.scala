package de.uni_potsdam.hpi.coheel.programs

import java.lang.Iterable
import java.util

import org.apache.flink.api.common.functions.{RichGroupReduceFunction, RichMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

abstract class BestPracticesProgram extends NoParamCoheelProgram {

	override def getDescription: String = "Best Practices"


	override def buildProgram(env: ExecutionEnvironment): Unit = {
//		illustrateExecuteName(env)
		illustrateMapPartition(env)
//		illustrateGroupBy(env)



		illustrateRichFunctions(env)
	}

	def illustrateExecuteName(env: ExecutionEnvironment): Unit = {
		env.execute("Nice name which appears in the web view")

	}

	def illustrateMapPartition(env: ExecutionEnvironment): Unit = {
		val data = env.fromCollection(1 to 100)
		data.mapPartition { (partition: Iterator[Int], out: Collector[Double]) =>
			partition.foreach { v =>
				Thread.sleep(100) // to illustrate some long running computation
				out.collect(v / 2.0)
			}
		}

		// +: describe how it's useful in CohEEL
	}

	def illustrateGroupBy(env: ExecutionEnvironment): Unit = {
		val data = env.fromCollection(1 to 100)
		val grouped = data.groupBy { _ % 10 }.reduceGroup { group =>
			(group.size, group.sum) // Wrong!
		}

//		def reduceIntGroup(group: Iterator[Int]): (Int, Int) = {
//			(group.size, group.sum)
//		}

		grouped.print()
	}

	def illustrateRichFunctions(env: ExecutionEnvironment): Unit = {
		val data = env.fromCollection(1 to 100)

		val broadCastData = env.fromCollection(1 to 10)



		data.groupBy { _ % 10 == 0}
			.reduceGroup(new MyReducer)
			.withBroadcastSet(broadCastData, "BROADCASTDATA")
		// +: describe how it's useful in CohEEL
	}
	class MyReducer extends RichGroupReduceFunction[Int, Double] {
		var broadcastData: util.List[Int] = null

		override def open(params: Configuration): Unit = {
			val runtimeContext = getRuntimeContext
			runtimeContext.getNumberOfParallelSubtasks
			runtimeContext.getIndexOfThisSubtask
			runtimeContext.getIntCounter("c").add(1)
			broadcastData = runtimeContext.getBroadcastVariable[Int]("BROADCASTDATA")

		}
		override def reduce(iterable: Iterable[Int], collector: Collector[Double]): Unit = {

		}
	}

}
