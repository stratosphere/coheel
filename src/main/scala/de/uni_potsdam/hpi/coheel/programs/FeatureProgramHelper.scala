package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.ml.SecondOrderFeatures
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

/**
 * Helper routines for creating the features.
 * This can be used both from the training data generator and the classifier.
 */
object FeatureProgramHelper {

	def applyCoheelFunctions[T <: Info](allCandidates: Seq[Classifiable[T]])(featureLineIteratorFunction: FeatureLine => Unit): Unit = {
		val surfaceOrder = allCandidates.sortBy(-_.surfaceProb)
		val contextOrder = allCandidates.sortBy(-_.contextProb)
		if (allCandidates.size > 1) {
			val surfaceRank       = SecondOrderFeatures.rank.apply(surfaceOrder)(_.surfaceProb)
			val surfaceDeltaTops  = SecondOrderFeatures.deltaTop.apply(surfaceOrder)(_.surfaceProb)
			val surfaceDeltaSuccs = SecondOrderFeatures.deltaSucc.apply(surfaceOrder)(_.surfaceProb)
			val contextRank       = SecondOrderFeatures.rank.apply(contextOrder)(_.contextProb)
			val contextDeltaTops  = SecondOrderFeatures.deltaTop.apply(contextOrder)(_.contextProb)
			val contextDeltaSuccs = SecondOrderFeatures.deltaSucc.apply(contextOrder)(_.contextProb)

			surfaceOrder.zipWithIndex.foreach { case (classifiable, i) =>
				import classifiable._
				val stringInfo = List(id, surfaceRepr, candidateEntity) ::: info.stringInfo
				val features = List(
					surfaceProb, surfaceRank(i), surfaceDeltaTops(i), surfaceDeltaSuccs(i),
					contextProb, contextRank(i), contextDeltaTops(i), contextDeltaSuccs(i)
				) ::: classifiable.info.extraFeatures(classifiable)
				featureLineIteratorFunction(FeatureLine(stringInfo, features))
			}
		}
	}

	def buildFeaturesPerGroup[T <: Info : TypeInformation : ClassTag](prg: CoheelProgram[_], classifiables: DataSet[Classifiable[T]]): GroupedDataSet[Classifiable[T]] = {
		val surfaceProbs = prg.readSurfaceProbs()
		val languageModels = prg.readLanguageModels()


		val classifiablesWithCandidates = classifiables.join(surfaceProbs)
			.where { classifiable => classifiable.surfaceRepr }
			.equalTo { surfaceProb => surfaceProb.surface }
			.name("Join: Classifiable With Surface Probs")
			.map { joinResult => joinResult match {
				case (classifiable, SurfaceProb(_, candidateEntity, surfaceProb)) =>
					classifiable.copy(candidateEntity = candidateEntity, surfaceProb = surfaceProb)
			}
		}.name("Classifiable with Candidates")

		val baseScores = classifiablesWithCandidates.join(languageModels)
			.where("candidateEntity")
			.equalTo("pageTitle")
			.name("Join: Link Candidates with LMs")
			.map { joinResult => joinResult match {
				case (classifiableWithCandidate, languageModel) =>
					val modelSize = languageModel.model.size
					val contextProb = classifiableWithCandidate.context.map { word =>
						Math.log(languageModel.model.get(word) match {
							case Some(prob) => prob
							case None => 1.0 / modelSize
						})
					}.sum

					classifiableWithCandidate.copy(contextProb = contextProb)
			}
		}.name("Classifiable with Context Probs")

		val trainingData = baseScores.groupBy(_.id)

		trainingData
	}

}
