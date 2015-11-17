package de.uni_potsdam.hpi.coheel.ml

import de.uni_potsdam.hpi.coheel.programs.DataClasses.{ClassificationInfo, FeatureLine}
import weka.classifiers.Classifier
import weka.core.{Instances, Attribute, FastVector, Instance}
import scala.collection.mutable

object CoheelClassifier {

	val NUMBER_OF_FEATURES = 15
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
		val attrs = new FastVector(NUMBER_OF_FEATURES + 1)
		// basic features
		attrs.addElement(new Attribute("prom"))
		attrs.addElement(new Attribute("promRank"))
		attrs.addElement(new Attribute("promDeltaTop"))
		attrs.addElement(new Attribute("promDeltaSucc"))
		attrs.addElement(new Attribute("context"))
		attrs.addElement(new Attribute("contextRank"))
		attrs.addElement(new Attribute("contextDeltaTop"))
		attrs.addElement(new Attribute("contextDeltaSucc"))
		// pos tags
		attrs.addElement(new Attribute("NN"))
		attrs.addElement(new Attribute("NNP"))
		attrs.addElement(new Attribute("JJ"))
		attrs.addElement(new Attribute("VB"))
		attrs.addElement(new Attribute("CD"))
		attrs.addElement(new Attribute("SYM"))
		attrs.addElement(new Attribute("W"))

		val classAttrValues = new FastVector(2)
		classAttrValues.addElement("0.0")
		classAttrValues.addElement("1.0")
		val classAttr = new Attribute("class", classAttrValues)
		attrs.addElement(classAttr)
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
		val instance = new Instance(1.0, attValues)
		instance
	}
}
