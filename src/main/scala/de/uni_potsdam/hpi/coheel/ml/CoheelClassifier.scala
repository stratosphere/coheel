package de.uni_potsdam.hpi.coheel.ml

import de.uni_potsdam.hpi.coheel.programs.DataClasses.FeatureLine
import weka.classifiers.Classifier
import weka.core.Instance
import scala.collection.mutable

class CoheelClassifier(classifier: Classifier) {

	val NUMBER_OF_FEATURES = 15
	val POSITIVE_CLASS = 1.0
	/**
	 * Classifies a given group of instances, which result from the same link/trie hit in the original text
	 * @param featureLine The features of all possible links.
	 * @return The predicted link or None, if no link is predicted.
	 */
	def classifyResults(featureLine: mutable.ArrayBuffer[FeatureLine]): Option[FeatureLine] = {
		var positivePredictions = List[FeatureLine]()
		featureLine.foreach { featureLine =>
			assert(featureLine.features.size == NUMBER_OF_FEATURES)
			val instance = buildInstance(featureLine)
			if (classifier.classifyInstance(instance) == POSITIVE_CLASS) {
				positivePredictions ::= featureLine
			}
		}
		if (positivePredictions.size == 1)
			positivePredictions.headOption
		else
			None
	}

	private def buildInstance(featureLine: FeatureLine): Instance = {
		val attValues = featureLine.features.toArray
		val instance = new Instance(1.0, attValues)
		instance
	}
}
