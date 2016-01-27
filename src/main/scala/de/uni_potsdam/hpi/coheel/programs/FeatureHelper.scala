package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.ml.SecondOrderFeatures
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import de.uni_potsdam.hpi.coheel.io.OutputFiles._

import scala.reflect.ClassTag

/**
 * Helper routines for creating the features.
 * This can be used both from the training data generator and the classifier.
 */
object FeatureHelper {

	val TRIE_HIT_MARKER = "TH"
	val LINK_MARKER = "L"

	import CoheelLogger._

	def applyCoheelFunctions[T <: Info](allCandidates: Seq[Classifiable[T]])(featureLineIteratorFunction: FeatureLine[T] => Unit): Unit = {
		val allCandidatesWithIndex = allCandidates.zipWithIndex
		val surfaceOrder = allCandidatesWithIndex.sortBy(-_._1.surfaceProb)
		val contextOrder = allCandidatesWithIndex.sortBy(-_._1.contextProb)
		val surfaceRank = SecondOrderFeatures.rank.apply(surfaceOrder)(_.surfaceProb)
		val surfaceDeltaTops = SecondOrderFeatures.deltaTop.apply(surfaceOrder)(_.surfaceProb)
		val surfaceDeltaSuccs = SecondOrderFeatures.deltaSucc.apply(surfaceOrder)(_.surfaceProb)
		val contextRank = SecondOrderFeatures.rank.apply(contextOrder)(_.contextProb)
		val contextDeltaTops = SecondOrderFeatures.deltaTop.apply(contextOrder)(_.contextProb)
		val contextDeltaSuccs = SecondOrderFeatures.deltaSucc.apply(contextOrder)(_.contextProb)

		allCandidatesWithIndex.foreach { case (classifiable, i) =>
			import classifiable._
			val features = List(
				surfaceProb, surfaceRank(i), surfaceDeltaTops(i), surfaceDeltaSuccs(i),
				contextProb, contextRank(i), contextDeltaTops(i), contextDeltaSuccs(i),
				surfaceLinkProb
			) ::: classifiable.info.furtherFeatures(classifiable)
			featureLineIteratorFunction(FeatureLine[T](id, surfaceRepr, candidateEntity, classifiable.info, features))
		}
	}

	def buildFeaturesPerGroup[T <: Info : TypeInformation : ClassTag](env: ExecutionEnvironment, classifiables: DataSet[Classifiable[T]]): GroupedDataSet[Classifiable[T]] = {
		val surfaceProbs = readSurfaceProbs(env)
		val languageModels = readLanguageModels(env)

		val classifiablesWithCandidates: DataSet[DataClasses.Classifiable[T]] = classifiables.join(surfaceProbs)
			.where("surfaceRepr")
			.equalTo("surface")
			.name("Join: Classifiable With Surface Probs")
			.map { joinResult => joinResult match {
				case (classifiable, SurfaceProb(_, candidateEntity, surfaceProb)) =>
					// enrich classifiable with possible candidate entities and their surface probabilities
					classifiable.withCandidateEntityAndSurfaceProb(candidateEntity, surfaceProb)
			}
		}.name("Classifiable with Candidates")

//		classifiablesWithCandidates.map { c =>
//			(c.id, c.surfaceRepr, c.candidateEntity, c.surfaceProb, c.info, c.context.deep)
//		} .writeAsTsv(debug1Path)

		val baseScores = classifiablesWithCandidates.join(languageModels)
			.where("candidateEntity")
			.equalTo("pageTitle")
			.name("Join: Link Candidates with LMs")
			.map { joinResult => joinResult match {
				case (classifiableWithCandidate, languageModel) =>
					val contextProb = classifiableWithCandidate.context.map { word =>
						Math.log(languageModel.prob(word))
					}.sum
					classifiableWithCandidate.withContextProb(contextProb)
			}
		}.name("Classifiable with Context Probs")

//		classifiablesWithCandidates.map { c =>
//			(c.id, c.surfaceRepr, c.candidateEntity, c.surfaceProb, c.info, c.contextProb)
//		}.writeAsTsv(debug2Path)

		val trainingData = baseScores.groupBy(_.id)

		trainingData
	}

	private def readSurfaceProbs(env: ExecutionEnvironment, threshold: Double = 0.0): DataSet[SurfaceProb] = {
		env.readTextFile(surfaceProbsPath).flatMap { line =>
			val split = line.split('\t')
			if (split.length > 1) {
				val tokens = split(0).split(' ')
				if (tokens.nonEmpty) {
					val prob = split(2).toDouble
					if (prob > threshold)
						Some(SurfaceProb(split(0), split(1), prob))
					else
						None
				}
				else
					None
			}
			else None
		}.name("Read surface probs")
	}

	private def readLanguageModels(env: ExecutionEnvironment): DataSet[LanguageModel] = {
		env.readTextFile(languageModelsPath).flatMap { line =>
			val lineSplit = line.split('\t')
			val pageTitle = lineSplit(0)
			if (lineSplit.length < 2) {
				log.warn(s"$pageTitle not long enough: $line")
				None
			} else {
				val model = lineSplit(1).split(' ').flatMap { entrySplit =>
					val wordSplit = entrySplit.split('\0')
					if (wordSplit.length == 2)
						Some(wordSplit(0), wordSplit(1).toInt)
					else
						None
				}.toMap
				Some(LanguageModel(pageTitle, model))
			}
		}.name("Reading language models")
	}

}
