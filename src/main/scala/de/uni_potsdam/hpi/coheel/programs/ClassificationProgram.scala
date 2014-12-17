package de.uni_potsdam.hpi.coheel.programs

import org.apache.flink.api.scala._
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.programs.DataClasses._

class ClassificationProgram extends CoheelProgram {

	override def getDescription: String = "CohEEL Classification"

	val data = """|Angela Dorothea Merkel (German: [aŋˈɡeːla doʁoˈteːa ˈmɛʁkl̩] ( listen);[1] née Kasner; born 17 July 1954) is
			      |a German politician and a former research scientist who has been the leader of the Christian Democratic Union (CDU)
		          |since 2000 and the Chancellor of Germany since 2005. She is the first woman to hold either office.""".stripMargin

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val toTag = env.fromElements(data)
//		val surfaceProbs = env.readCsvFile(surfaceProbsPath, "\n", '\t', false, false, Array[Int](0))
		val surfaceProbs = env.readTextFile(surfaceProbsPath).map { line =>
			val split = line.split('\t')
			SurfaceProbLink(split(0), split(1), split(2).toDouble)
		}

		val result = toTag.crossWithHuge(surfaceProbs).flatMap { value =>
			val (text, link) = value
			val surface = link.surface
			if (text.contains(surface))
				List(link)
			else
				List()
		}
//			.filter(new FilterFunction[(String, String)] {
//			override def filter(value: (String, String)): Boolean = {
//				val (text, link) = value
//				val surface = link.split('\t')(0)
//				text.contains
//
//			}
//		})
//		{ (text, link) =>
//			(text, link)
//		}

//		result.filter { case (e: String, f: String) =>
//			true
//		}


		result.print()
	}

}
