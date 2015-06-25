package de.uni_potsdam.hpi.coheel.programs

import java.lang.Iterable
import java.util
import de.uni_potsdam.hpi.coheel.io.OutputFiles._

import org.apache.flink.api.common.functions.{RichGroupReduceFunction, RichMapFunction}
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector
import scala.collection.JavaConversions._


class BestPracticesProgram extends NoParamCoheelProgram with Serializable {

	override def getDescription: String = "Best Practices"


	override def buildProgram(env: ExecutionEnvironment): Unit = {
//		illustrateNaming(env)
//		illustrateGroupBy(env)
//		illustrateMapPartition(env)
//		illustrateRichFunctions(env)
//		illustrateTests(env)

		illustrateIteration(env)
	}

	def illustrateNaming(env: ExecutionEnvironment): Unit = {
		val data = env.fromCollection(1 to 100).name("Input-Data")
		val result = data.map(_ * 2).name("Doubled")
		result.writeAsText(bestPracticesPath, FileSystem.WriteMode.OVERWRITE)
		env.execute("Nice name which appears in the web view")
	}

	case class SomeDataClass(size: Int, sum: Int) // can be written to file easily
	def illustrateGroupBy(env: ExecutionEnvironment): Unit = {
		val data = env.fromCollection(1 to 100)
		val grouped = data
			.groupBy { _ % 10 }
			.reduceGroup(reduceIntGroup _)

		grouped.groupBy { el => el._1 }

		grouped.print()
//		grouped.writeAsCsv(bestPracticesPath, writeMode = FileSystem.WriteMode.OVERWRITE)


		// Show debugging
	}

	def reduceIntGroup(groupIt: Iterator[Int]): (Int, Int) = {
		val group = groupIt.toList
		(group.size, group.sum)
	}

	def illustrateMapPartition(env: ExecutionEnvironment): Unit = {
		val data = env.fromCollection(1 to 100)
		data.mapPartition { (partition: Iterator[Int], out: Collector[Double]) =>
			Thread.sleep(100) // to illustrate some long running computation
			partition.foreach { v =>
				out.collect(v / 2.0)
			}
		}

		data.print()

		// +: describe how it's useful in CohEEL
	}

	def illustrateRichFunctions(env: ExecutionEnvironment): Unit = {
		val data = env.fromCollection(1 to 100)

		val broadCastData = env.fromCollection(1 to 10)



		val result = data.groupBy { _ % 10 }
			.reduceGroup(new MyReducer)
			.withBroadcastSet(broadCastData, "BROADCASTDATA")
		// +: describe how it's useful in CohEEL
		result.print()
	}

	class MyReducer extends RichGroupReduceFunction[Int, Double] {
		var broadcastData: util.List[Int] = null

		override def open(params: Configuration): Unit = {
			val runtimeContext = getRuntimeContext
			runtimeContext.getNumberOfParallelSubtasks
			runtimeContext.getIndexOfThisSubtask
			val counter = runtimeContext.getIntCounter("c")
			broadcastData = runtimeContext.getBroadcastVariable[Int]("BROADCASTDATA")
			println(s"BD size: ${broadcastData.size}")

		}
		override def reduce(group: Iterable[Int], out: Collector[Double]): Unit = {
			group.zip(broadcastData).foreach { case (groupValue, broadcastValue) =>
				out.collect(groupValue.toDouble / broadcastValue)
			}
		}
	}

	def illustrateIteration(env: ExecutionEnvironment): Unit = {
		// split programs into smaller parts, run on subset
//		for (i <- 10 to 100 by 10) {
//			val data = env.fromElements(i to (i + 10))
////			data.print()
//			env.execute(s"Iteration #$i")
//		}
	}

	def illustrateTests(env: ExecutionEnvironment): Unit = {
		env.fromElements("b").writeAsText(bestPracticesPath, FileSystem.WriteMode.OVERWRITE)
	}

}
