package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.Timer
import de.uni_potsdam.hpi.coheel.datastructures.NewTrie
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.programs.DataClasses.Plaintext
import de.uni_potsdam.hpi.coheel.wiki.{TokenizerHelper, WikiPage}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import scala.collection.mutable


object SurfaceEvaluationProgram {
	val BROADCAST_SURFACES = "surfaces"
}
class SurfaceEvaluationProgram extends CoheelProgram[Int] {

	override def getDescription = "Surface Evaluation"

	val SUBSET_NUMBER = 10
	val SPECIAL_CASE_NUMBER = -1

	val params = if (runsOffline()) Seq(0) else (1 to SUBSET_NUMBER) :+ SPECIAL_CASE_NUMBER

	override def buildProgram(env: ExecutionEnvironment, param: Int): Unit = {
		if (param == SPECIAL_CASE_NUMBER)
			summarizeEvaluation()
		else
			runEvaluationOnSubset(param)
	}

	def runEvaluationOnSubset(param: Int): Unit = {
		val currentFile = if (runsOffline()) "" else s"/$param"
		val surfaceLinkProbs = getSurfaceLinkProbs(currentFile)
		val plainTexts = getPlainTexts()

		val surfaceEvaluationPerDocument = plainTexts
			.flatMap(new SurfaceEvaluationFlatMap)
			.withBroadcastSet(surfaceLinkProbs, EntireTextSurfacesProgram.BROADCAST_SURFACES)
			.name("Surface-Evaluation-Per-Document")

		val evaluations = surfaceEvaluationPerDocument.map(_._2)
		val surfaceEvaluationPerSubset = aggregateEvaluations(evaluations).name("Surface-Evaluation-Per-Subset")

		surfaceEvaluationPerDocument.writeAsTsv(surfaceEvaluationPerDocumentPath + currentFile)
		surfaceEvaluationPerSubset.writeAsTsv(surfaceEvaluationPerSubsetPath + currentFile)
	}

	def summarizeEvaluation(): Unit = {
		val conf = new Configuration
		conf.setBoolean("recursive.file.enumeration", true)
		val evaluations = environment.readCsvFile[Evaluation](surfaceEvaluationPerSubsetPath, "\n", '\t').withParameters(conf)

		val finalSurfaceEvaluation = aggregateEvaluations(evaluations).map { evaluation =>
			import evaluation._
			(evaluation.threshold, precision(), precisionWithoutSubsetFps(), recall(), f1())
		}
		finalSurfaceEvaluation.writeAsTsv(surfaceEvaluationPath)
	}


	private def aggregateEvaluations(evaluations: DataSet[Evaluation]): DataSet[Evaluation] = {
		evaluations.groupBy { evaluation =>
			evaluation.threshold
		}
		.reduce { (eval1, eval2) =>
			Evaluation(
				eval1.threshold,
				eval1.actualSurfaces + eval2.actualSurfaces,
				eval1.potentialSurfaces + eval2.potentialSurfaces,
				eval1.tp + eval2.tp,
				eval1.fp + eval2.fp,
				eval1.subsetFp + eval2.subsetFp,
				eval1.fn + eval2.fn
			)
		}
	}
}

case class Evaluation(threshold: String, actualSurfaces: Int, potentialSurfaces: Int, tp: Int, fp: Int, subsetFp: Int, fn: Int) {
	override def toString: String = {
		s"Evaluation(threshold=$threshold,actualSurfaces=$actualSurfaces,potentialSurfaces=$potentialSurfaces,tp=$tp,fp=$fp,subsetFp=$subsetFp,fn=$fn)"
	}

	def precision(): Double = {
		tp.toDouble / (tp.toDouble + fp.toDouble)
	}
	def precisionWithoutSubsetFps(): Double = {
		tp.toDouble / (tp.toDouble + fp.toDouble - subsetFp)
	}
	def recall(): Double = {
		tp.toDouble / (tp.toDouble + fn.toDouble)
	}
	def f1(): Double = {
		2 * precision() * recall() / (precision() + recall())
	}
}


class SurfaceEvaluationFlatMap extends RichFlatMapFunction[Plaintext, (String, Evaluation)] {

	var trie: NewTrie = _

	override def open(params: Configuration): Unit = {
		trie = getRuntimeContext.getBroadcastVariableWithInitializer(EntireTextSurfacesProgram.BROADCAST_SURFACES, new TrieWithProbBroadcastInitializer)
	}
	override def flatMap(plainText: Plaintext, out: Collector[(String, Evaluation)]): Unit = {
		// determine the actual surfaces, from the real wikipedia article
		val actualSurfaces = plainText.linkString.split(CoheelProgram.LINK_SPLITTER).map(_.split(" ").toSeq).toSet

		// determine potential surfaces, i.e. the surfaces that the NER would return
		Timer.start("FINDALL IN TRIE")
		var potentialSurfacesWithProbs = trie.findAllInWithProbs(plainText.plainText)
			.map { case (surface, prob) => (surface.split(' ').toSeq, prob) }
			.to[mutable.MutableList]
		Timer.end("FINDALL IN TRIE")

		if (plainText.pageTitle == "My test article") {
			println(potentialSurfacesWithProbs.toList)
		}

		val subSetCheck = mutable.Map[Seq[String], Boolean]()
		(0.05f to 0.95f by 0.05f).foreach { threshold =>
			Timer.start("FILTER DOWN")
			potentialSurfacesWithProbs = potentialSurfacesWithProbs.filter(_._2 >= threshold)
			Timer.end("FILTER DOWN")
			Timer.start("BUILD SET")
			val potentialSurfaces = potentialSurfacesWithProbs.map(_._1).toSet
			Timer.end("BUILD SET")

			Timer.start("TP")
			// TPs are those surfaces, which are actually in the text and our system would return it
			val tp = actualSurfaces.intersect(potentialSurfaces)
			Timer.end("TP")
			// FPs are those surfaces, which are returned but are not actually surfaces
			Timer.start("FP")
			val fp = potentialSurfaces.diff(actualSurfaces)
			Timer.end("FP")

			Timer.start("SUBSET FP")
			val subsetFp = fp.count { fpSurface =>
				subSetCheck.get(fpSurface) match {
					case Some(result) =>
						result
					case None =>
						val res = extractSubsetFp(actualSurfaces, fpSurface)
						subSetCheck(fpSurface) = res
						res
				}
			}
			Timer.end("SUBSET FP")

			Timer.start("FN")
			// FN are those surfaces, which are actual surfaces, but are not returned
			val fn = actualSurfaces.diff(potentialSurfaces)
			Timer.end("FN")
			out.collect(plainText.pageTitle, Evaluation(f"$threshold%.2f", actualSurfaces.size, potentialSurfaces.size, tp.size, fp.size, subsetFp, fn.size))
		}
	}

	def extractSubsetFp(tp: Set[Seq[String]], fpSurface: Seq[String]): Boolean = {
		tp.exists { tpSurface =>
			tpSurface.containsSlice(fpSurface)
		}
	}

	override def close(): Unit = {
		Timer.printAll()
	}
}
