package de.uni_potsdam.hpi.coheel.ml

import java.util

import de.uni_potsdam.hpi.coheel.programs.DataClasses.{ClassificationInfo, FeatureLine}
import weka.classifiers.Classifier
import weka.core.Attribute
import weka.core.Instances
import weka.core.Instance
import weka.core.DenseInstance

object CoheelClassifier {

	val NUMBER_OF_FEATURES = 16 // excluding class attribute
	val POSITIVE_CLASS = 1.0

	val POS_TAG_GROUPS = Array(
		List("NN", "NNS"),
		List("NNP", "NNPS"),
		List("JJ", "JJR", "JJS"),
		List("VB", "VBD", "VBG", "VBN", "VBP", "VBZ"),
		List("CD"),
		List("SYM"),
		List("WDT", "WP", "WP$", "WRB")
	)

	val FEATURE_DEFINITION = {
		val attrs = new util.ArrayList[Attribute](NUMBER_OF_FEATURES + 1)
		// basic features
		attrs.add(new Attribute("prom"))
		attrs.add(new Attribute("promRank"))
		attrs.add(new Attribute("promDeltaTop"))
		attrs.add(new Attribute("promDeltaSucc"))
		attrs.add(new Attribute("context"))
		attrs.add(new Attribute("contextRank"))
		attrs.add(new Attribute("contextDeltaTop"))
		attrs.add(new Attribute("contextDeltaSucc"))
		attrs.add(new Attribute("surfaceLinkProb"))
		// pos tags
		attrs.add(new Attribute("NN"))
		attrs.add(new Attribute("NNP"))
		attrs.add(new Attribute("JJ"))
		attrs.add(new Attribute("VB"))
		attrs.add(new Attribute("CD"))
		attrs.add(new Attribute("SYM"))
		attrs.add(new Attribute("W"))

		val classAttrValues = new util.ArrayList[String](2)
		classAttrValues.add("0.0")
		classAttrValues.add("1.0")
		val classAttr = new Attribute("class", classAttrValues)
		attrs.add(classAttr)
		attrs
	}
}

class CoheelClassifier(classifier: Classifier) {

	val instances = new Instances("Classification", CoheelClassifier.FEATURE_DEFINITION, 1)
	instances.setClassIndex(CoheelClassifier.NUMBER_OF_FEATURES)

	/**
	 * Classifies a given group of instances, which result from the same link/trie hit in the original text.
	 * Only if exactly one true prediction is given, the function returns a result.
	 * @param featureLine The features of all possible links.
	 * @return The predicted link or None, if no link is predicted.
	 */
	def classifyResultsWithSeedLogic(featureLine: Seq[FeatureLine[ClassificationInfo]]): Option[FeatureLine[ClassificationInfo]] = {
		var positivePredictions = List[FeatureLine[ClassificationInfo]]()
		featureLine.foreach { featureLine =>
			assert(featureLine.features.size == CoheelClassifier.NUMBER_OF_FEATURES || featureLine.features.size == CoheelClassifier.NUMBER_OF_FEATURES + 1)
			val instance = buildInstance(featureLine)
			instance.setDataset(instances)
			if (classifier.classifyInstance(instance) == CoheelClassifier.POSITIVE_CLASS) {
				positivePredictions ::= featureLine
			}
			// TODO: Temporary
			if (List(
				"L-0468265056-00000626-2281324111",
				"L-0468265056-00000627-4156867526",
				"L-0468265056-00000628-1482103611",
				"L-0468265056-00000629-0312499337"
			).contains(featureLine.id) || featureLine.id.startsWith("TH-")) {
				println("Feature Line:")
				println(featureLine)
				println("Classification")
				print(classifier.classifyInstance(instance))
				val s = if (instance.numAttributes() == CoheelClassifier.NUMBER_OF_FEATURES) "" else " " + (instance.classValue() == classifier.classifyInstance(instance))
				println(s)
				println("O" * 80)
			}
		}
		if (positivePredictions.size == 1)
			positivePredictions.headOption
		else
			None
	}

	/**
	 * Classifies a given group of instances, which result from the same link/trie hit in the original text, using candidate logic.
	 */
	def classifyResultsWithCandidateLogic(featureLine: Seq[FeatureLine[ClassificationInfo]]): List[FeatureLine[ClassificationInfo]] = {
		var positivePredictions = List[FeatureLine[ClassificationInfo]]()
		featureLine.foreach { featureLine =>
			assert(featureLine.features.size == CoheelClassifier.NUMBER_OF_FEATURES || featureLine.features.size == CoheelClassifier.NUMBER_OF_FEATURES + 1)
			val instance = buildInstance(featureLine)
			instance.setDataset(instances)
			if (classifier.classifyInstance(instance) == CoheelClassifier.POSITIVE_CLASS) {
				positivePredictions ::= featureLine
			}
		}
		positivePredictions
	}

	private def buildInstance(featureLine: FeatureLine[ClassificationInfo]): Instance = {
		val attValues = featureLine.features.toArray
		val instance = new DenseInstance(1.0, attValues)
		instance
	}
}
