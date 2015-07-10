package de.uni_potsdam.hpi.coheel.ml

import de.uni_potsdam.hpi.coheel.programs.DataClasses.{ClassificationInfo, FeatureLine}
import weka.classifiers.Classifier
import weka.core.{FastVector, Attribute, Instances, Instance, DenseInstance}
import scala.collection.mutable

object CoheelClassifier {
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
		val attrs = new FastVector[Attribute](17)
		attrs.addElement(new Attribute("id"))

		attrs.addElement(new Attribute("NN"))
		attrs.addElement(new Attribute("NNP"))
		attrs.addElement(new Attribute("JJ"))
		attrs.addElement(new Attribute("VB"))
		attrs.addElement(new Attribute("CD"))
		attrs.addElement(new Attribute("SYM"))
		attrs.addElement(new Attribute("W"))

		attrs.addElement(new Attribute("prom"))
		attrs.addElement(new Attribute("promRank"))
		attrs.addElement(new Attribute("promDeltaTop"))
		attrs.addElement(new Attribute("promDeltaSucc"))
		attrs.addElement(new Attribute("context"))
		attrs.addElement(new Attribute("contextRank"))
		attrs.addElement(new Attribute("contextDeltaTop"))
		attrs.addElement(new Attribute("contextDeltaSucc"))
		val classAttrValues = new FastVector[String](2)
		classAttrValues.addElement("0.0")
		classAttrValues.addElement("1.0")
		val classAttr = new Attribute("class", classAttrValues)
		attrs.addElement(classAttr)
		attrs
	}
	val FEATURE_DEFINITION_WITHOUT_ID = {
		val newDefinition = FEATURE_DEFINITION.copy()//.asInstanceOf[FastVector[Attribute]]
		// remove id
		newDefinition.removeElementAt(0)
//		// remove class attribute
//		newDefinition.removeElementAt(newDefinition.size() - 1)
		newDefinition
	}
}

class CoheelClassifier(classifier: Classifier) {

	val NUMBER_OF_FEATURES = 15
	val POSITIVE_CLASS = 1.0

	val instances = new Instances("Classification", CoheelClassifier.FEATURE_DEFINITION_WITHOUT_ID, 1)
	instances.setClassIndex(NUMBER_OF_FEATURES)

	/**
	 * Classifies a given group of instances, which result from the same link/trie hit in the original text
	 * @param featureLine The features of all possible links.
	 * @return The predicted link or None, if no link is predicted.
	 */
	def classifyResults(featureLine: mutable.ArrayBuffer[FeatureLine[ClassificationInfo]]): Option[FeatureLine[ClassificationInfo]] = {
		var positivePredictions = List[FeatureLine[ClassificationInfo]]()
		featureLine.foreach { featureLine =>
			assert(featureLine.features.size == NUMBER_OF_FEATURES)
			val instance = buildInstance(featureLine)
			instance.setDataset(instances)
			if (classifier.classifyInstance(instance) == POSITIVE_CLASS) {
				positivePredictions ::= featureLine
			}
		}
		// TODO: Change to return only if there is _exactly_ one positive prediction
		if (positivePredictions.size >= 1)
			positivePredictions.headOption
		else
			None
	}

	private def buildInstance(featureLine: FeatureLine[ClassificationInfo]): Instance = {
		val attValues = featureLine.features.toArray
		val instance: Instance = new DenseInstance(1.0, attValues)
		instance
	}
}
