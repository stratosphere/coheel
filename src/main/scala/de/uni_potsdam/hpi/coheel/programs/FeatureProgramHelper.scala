package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.ml.SecondOrderFeatures
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import org.apache.flink.api.scala._

/**
 * Helper routines for creating the features.
 * This can be used both from the training data generator and the classifier.
 */
object FeatureProgramHelper {

	def applyCoheelFunctions(allCandidates: Seq[LinkWithScores])(featureLineIteratorFunction: FeatureLine => Unit): Unit = {
		val promOrder = allCandidates.sortBy(-_.promScore)
		val contextOrder = allCandidates.sortBy(-_.contextScore)
		if (allCandidates.size > 1) {
			val promRank       = SecondOrderFeatures.rank.apply(promOrder)(_.promScore)
			val promDeltaTops  = SecondOrderFeatures.deltaTop.apply(promOrder)(_.promScore)
			val promDeltaSuccs = SecondOrderFeatures.deltaSucc.apply(promOrder)(_.promScore)
			val contextRank       = SecondOrderFeatures.rank.apply(contextOrder)(_.contextScore)
			val contextDeltaTops  = SecondOrderFeatures.deltaTop.apply(contextOrder)(_.contextScore)
			val contextDeltaSuccs = SecondOrderFeatures.deltaSucc.apply(contextOrder)(_.contextScore)

			promOrder.zipWithIndex.foreach { case (candidate, i) =>
				val positiveInstance = if (candidate.destination == candidate.candidateEntity) 1.0 else 0.0
				import candidate._
				val stringInfo = List(fullId, surfaceRepr, source, candidateEntity)
				val features = posTagScores.toList ::: List(
					promScore, promRank(i), promDeltaTops(i), promDeltaSuccs(i),
					contextScore, contextRank(i), contextDeltaTops(i), contextDeltaSuccs(i),
					positiveInstance
				)
				featureLineIteratorFunction(FeatureLine(stringInfo, features))
			}
		}
	}

	def buildFeaturesPerGroup(prg: CoheelProgram[_], linksWithContext: DataSet[LinkWithContext]): GroupedDataSet[LinkWithScores] = {
		val surfaceProbs = prg.readSurfaceProbs()
		val languageModels = prg.readLanguageModels()

		val posTagGroups = Array(
			List("NN", "NNS"),
			List("NNP", "NNPS"),
			List("JJ", "JJR", "JJS"),
			List("VB", "VBD", "VBG", "VBN", "VBP", "VBZ"),
			List("CD"),
			List("SYM"),
			List("WDT", "WP", "WP$", "WRB")
		)


		val linkCandidates = linksWithContext.join(surfaceProbs)
			.where { linkWithContext => linkWithContext.surfaceRepr }
			.equalTo { surfaceProb => surfaceProb.surface }
			.name("Join: Links With Surface Probs")
			.map { joinResult => joinResult match {
			case (linkWithContext, SurfaceProb(_, candidateEntity, prob)) =>
				import linkWithContext._
				LinkCandidate(fullId, surfaceRepr, source, destination, candidateEntity, prob, context,
					posTagGroups.map { group => if (group.exists(posTags.contains(_))) 1 else 0 })
		}
		}.name("Link Candidates")

		val baseScores = linkCandidates.join(languageModels)
			.where("candidateEntity")
			.equalTo("pageTitle")
			.name("Join: Link Candidates with LMs")
			.map { joinResult => joinResult match {
				case (linkCandidate, languageModel) =>
					val modelSize = languageModel.model.size
					val contextProb = linkCandidate.context.map { word =>
						Math.log(languageModel.model.get(word) match {
							case Some(prob) => prob
							case None => 1.0 / modelSize
						})
					}.sum

					import linkCandidate._

					LinkWithScores(fullId, surfaceRepr, source, destination, candidateEntity, posTagsScores.map(_.toDouble), prob, contextProb)
			}
		}.name("Links with Scores")

		val trainingData = baseScores.groupBy(_.fullId)

		trainingData
	}

}
