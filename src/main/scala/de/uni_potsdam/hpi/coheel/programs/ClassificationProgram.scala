package de.uni_potsdam.hpi.coheel.programs

import org.apache.flink.api.scala._
import de.uni_potsdam.hpi.coheel.io.OutputFiles._

class ClassificationProgram extends CoheelProgram {

	override def getDescription: String = "CohEEL Classification"

	val data = """|Angela Dorothea Merkel (German: [aŋˈɡeːla doʁoˈteːa ˈmɛʁkl̩] ( listen);[1] née Kasner; born 17 July 1954) is
			      |a German politician and a former research scientist who has been the leader of the Christian Democratic Union (CDU)
		          |since 2000 and the Chancellor of Germany since 2005. She is the first woman to hold either office.""".stripMargin

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val toTag = env.fromElements(data)
//		val surfaceProbs = env.readCsvFile(surfaceProbsPath, "\n", '\t', false, false, Array[Int](0))
		val surfaceProbs = env.readTextFile(surfaceProbsPath)

		val result = toTag.crossWithHuge(surfaceProbs) { (text, link) =>

			link
		}


		result.first(3).print()
	}

}
