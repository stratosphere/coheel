package de.uni_potsdam.hpi.coheel.ml

import java.io.File

import weka.classifiers.{Classifier, Evaluation}
import weka.classifiers.functions.{SMO, MultilayerPerceptron, SimpleLogistic, Logistic}
import weka.classifiers.trees.{RandomForest, J48}
import weka.core.Instance
import weka.core.converters.CSVLoader

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random


object WekaLearner {

	def main(args: Array[String]) = {
		val r = new Random(21011991)
		val instanceGroups = readInstancesInGroups()
		val groupsCount = instanceGroups.size
		println(s"There are $groupsCount instance groups.")

		val randomOrder = r.shuffle(instanceGroups)
		val trainingInstances = randomOrder.take((groupsCount * 0.7).toInt)
		val testInstances     = randomOrder.drop((groupsCount * 0.7).toInt)

//		val loader = new CSVLoader()
//		loader.setMissingValue("NaN")
//		loader.setSource(new File("cluster-output/raw-scores.tsv"))
//		val instances = loader.getDataSet
//		instances.setClassIndex(8)
//		println("=")
//		println(instances.instance(0).classValue())
//		println(instances.instance(1).classValue())
//		println("=")
//		println(instances.numAttributes())
//		println(instances.numInstances())



		// CostSensitiveClassifier
		val classifiers = List(
			new J48,
			new Logistic,
			new SimpleLogistic,
			new MultilayerPerceptron,
			new RandomForest,
			new SMO
		)

//		Classifier.forName("weka.classifiers.trees.J48", Array[String]("-C", "0.25", "-M", "2"))

//		classifiers.foreach { classifier =>
//			classifier.buildClassifier(instances)
//			val evaluation = new Evaluation(trainInstances);
//			evaluation.evaluateModel(scheme, testInstances);
//			System.out.println(evaluation.toSummaryString());
//
//		}


		// context size fix
		// missing values

		// cost model

		// surface-link-at-all probability?
		// context < 100 ==>  Missing value
	}

	def buildInstance(split: Array[String]): Instance = {
		val attValues = split.map(_.toDouble).array
		val instance = new Instance(1.0, attValues)
		instance
	}

	def readInstancesInGroups(): ArrayBuffer[ArrayBuffer[Instance]] = {
		val scoresFile = new File("cluster-output/raw-scores.tsv")
		val scoresSource = Source.fromFile(scoresFile)
		val groups = ArrayBuffer[ArrayBuffer[Instance]]()
		var currentGroup = ArrayBuffer[Instance]()
		var lastId: Int = -1
		val lines = scoresSource.getLines()
		lines.drop(1)
		lines.foreach { line =>
			val split = line.split("\t")
			val id = split.head.toInt
			if (id != lastId && currentGroup.nonEmpty) {
				groups += currentGroup.clone()
				currentGroup.clear()
				lastId = id
			}
			currentGroup += buildInstance(split)
		}
		groups
	}
}
