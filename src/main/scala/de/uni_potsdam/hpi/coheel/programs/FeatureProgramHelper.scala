package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import org.apache.flink.api.scala._

/**
 * Helper routines for creating the features.
 * This can be used both from the training data generator and the classifier.
 */
object FeatureProgramHelper {

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
