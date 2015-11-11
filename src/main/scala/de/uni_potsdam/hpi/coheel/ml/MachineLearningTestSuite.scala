package de.uni_potsdam.hpi.coheel.ml

import java.io.File

import de.uni_potsdam.hpi.coheel.util.Timer
import weka.classifiers.CostMatrix
import weka.classifiers.bayes.NaiveBayes
import weka.classifiers.functions.{Logistic, MultilayerPerceptron, SimpleLogistic}
import weka.classifiers.meta.CostSensitiveClassifier
import weka.classifiers.trees.{J48, RandomForest}
import weka.core._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.{Failure, Random, Success, Try}


case class TrainingData(id: String,
                        surface: String,
                        candidateEntity: String,
                        source: String,
                        destination: String)

class CoheelInstance(weight: Double, val attValues: Array[Double], val info: TrainingData)
	extends Instance(weight, attValues) {

	def this(other: CoheelInstance) = this(other.weight(), other.toDoubleArray, other.info)

	override def copy(): AnyRef = {
		val result = new CoheelInstance(this)
		result.m_Dataset = m_Dataset
		result
	}
}

object MachineLearningTestSuite {

	val r = new Random(21011991)

	val CLASS_INDEX = 15

	def main(args: Array[String]) = {
		val (train, test) = readTrainingDataAndBuildInstances()
		val expected = test.enumerateInstances().asScala.flatMap { case instance: CoheelInstance =>
			if (instance.classValue() == 1.0)
				// trie hit id, and correct entity
				Some((instance.info.id, instance.info.destination))
			else
				None
		}.toSet

		println("#" * 10 + " Test bare classifiers " + "#" * 10)
		testClassifiers(train, test, expected)


		println("#" * 10 + " Test classifiers with logic on top " + "#" * 10)
//		testCoheelClassifiers(train, test, expected)


		// missing values
		// surface-link-at-all probability?
		// context < 100 ==>  Missing value
	}

	def readTrainingDataAndBuildInstances(): (Instances, Instances) = {
		print("Reading .. "); Console.flush()
		val instanceGroups = r.shuffle(readInstancesInGroups())
		println("Done.")
		val groupsCount = instanceGroups.size
		val trainingRatio = (groupsCount * 0.7).toInt
		val (trainSet, testSet) = instanceGroups.splitAt(trainingRatio)
		println(s"There are $groupsCount instance groups, ${trainSet.size} in training and ${testSet.size} in test")
		println()

		val fullTrainingInstances = buildInstances("train", trainSet.flatten)
		val fullTestInstances = buildInstances("test", testSet.flatten)
		(fullTrainingInstances, fullTestInstances)
	}


	def buildInstance(split: Array[String]): Instance = {
		val id = split(0)
		val surface = split(1)
		val candidateEntity = split(2)
		val source = split(3)
		val destination = split(4)
		val attValues = split.slice(5, 5 + 15 + 1).map(_.toDouble)
		val trainingData = TrainingData(id, surface, candidateEntity, source, destination)

		val instance = new CoheelInstance(1.0, attValues, trainingData)
		instance
	}

	def readInstancesInGroups(): mutable.ArrayBuffer[mutable.ArrayBuffer[Instance]] = {
		val groups = mutable.ArrayBuffer[mutable.ArrayBuffer[Instance]]()

		val scoresSource = Source.fromFile(new File("output/training-data.wiki"))
		var currentGroup = mutable.ArrayBuffer[Instance]()
		var lastId: String = ""
		val lines = scoresSource.getLines()

		lines.foreach { line =>
			val split = line.split("\t")
			// id, surface, candidate entity, source, destination, 15 features, class
			assert(split.length == 21)
			val id = split.head
			if (id != lastId && currentGroup.nonEmpty) {
				groups += currentGroup
				currentGroup = mutable.ArrayBuffer[Instance]()
				lastId = id
			}
			currentGroup += buildInstance(split)
		}
		groups
	}

	def testClassifiers(train: Instances, test: Instances, expected: Set[(String, String)]): Unit = {
		classifiers.foreach { case (name, classifier) =>
			val runtimeTry = Try(Timer.timeFunction {
				classifier.buildClassifier(train)
			})
			runtimeTry match {
				case Failure(e) => println(s"    $name failed with ${e.getMessage}")
				case Success(runtime) =>
					println(s"    $name in ${msToMin(runtime.toInt)} min")
					val actual = mutable.Set[(String, String)]()
					test.enumerateInstances().asScala.foreach { case instance: CoheelInstance =>
						if (classifier.classifyInstance(instance) == 1.0) {
							actual.add((instance.info.id, instance.info.candidateEntity))
						}
					}
					val precision = expected.intersect(actual).size.toDouble / actual.size
					val recall = expected.intersect(actual).size.toDouble / expected.size
					println(f"      P: $precision%.3f, R: $recall%.3f, F1: ${2 * precision * recall / (precision + recall)}.3f")
			}
		}
		println("-" * 80)
	}

	def msToMin(t: Int): Int = {
		t / (1000 * 60)
	}

	def classifiers = {
//		println("----------- Rebuilding classifiers")
		val simpleLogistic = new SimpleLogistic
		simpleLogistic.setHeuristicStop(0)
		simpleLogistic.setMaxBoostingIterations(1500)
		simpleLogistic.setErrorOnProbabilities(true)

		val nb1 = new NaiveBayes
		nb1.setUseSupervisedDiscretization(true)
		val nb2 = new NaiveBayes
		nb2.setUseSupervisedDiscretization(false)

		val base = List(
			new Logistic,
			new J48,
			new SimpleLogistic,
//			simpleLogistic,
			new MultilayerPerceptron,
			nb1,
			nb2,
			new RandomForest
//			new SMO
		)
		base.flatMap { classifier =>
			val costMatrixFN = new CostMatrix(2)
			costMatrixFN.setElement(1, 0, 10)

			val costMatrixFP = new CostMatrix(2)
			costMatrixFP.setElement(0, 1, 10)

			val cost1 = new CostSensitiveClassifier
			cost1.setClassifier(classifier)
			cost1.setMinimizeExpectedCost(true)
			cost1.setCostMatrix(costMatrixFN)
			val cost2 = new CostSensitiveClassifier
			cost2.setClassifier(classifier)
			cost2.setCostMatrix(costMatrixFN)

			val cost3 = new CostSensitiveClassifier
			cost3.setClassifier(classifier)
			cost3.setMinimizeExpectedCost(true)
			cost3.setCostMatrix(costMatrixFP)
			val cost4 = new CostSensitiveClassifier
			cost4.setClassifier(classifier)
			cost4.setCostMatrix(costMatrixFP)

			val baseClassifierName = classifier.getClass.getSimpleName
			List(
				(baseClassifierName, classifier),
				(s"$baseClassifierName with 10 x FN cost, minimize expected cost = true", cost1),
				(s"$baseClassifierName with 10 x FN cost, minimize expected cost = false", cost2),
				(s"$baseClassifierName with 10 x FP cost, minimize expected cost = true", cost3),
				(s"$baseClassifierName with 10 x FP cost, minimize expected cost = false", cost4)
			)
		}
	}

	def buildInstances(name: String, instanceSeq: Seq[Instance]): Instances = {
		val instances = new Instances(name, CoheelClassifier.FEATURE_DEFINITION, instanceSeq.size)
		instanceSeq.foreach { inst =>
			instances.add(inst)
		}
		instances.setClassIndex(CLASS_INDEX)
		instances
	}

	/*
	def serializeGoodClassifier(fullTrainingInstances: Instances): Unit = {
		println("Serialize good classifier")
		println("=" * 80)
		// Build classifier
		val baseClassifier = new RandomForest
		//		baseClassifier.setPrintTrees(true)
		// Apply costs
		val classifier = new CostSensitiveClassifier
		classifier.setClassifier(baseClassifier)
		classifier.setMinimizeExpectedCost(true)
		val costMatrixFP = new CostMatrix(2)
		costMatrixFP.setElement(0, 1, 10)
		val costMatrixFN = new CostMatrix(2)
		costMatrixFN.setElement(1, 0, 10)
		classifier.setCostMatrix(costMatrixFN)
		// Train
		val filteredTraining = Filter.useFilter(fullTrainingInstances, removeFilter)
		classifier.buildClassifier(filteredTraining)
		// Serialize
		SerializationHelper.write("RandomForest-10FN.model", classifier)
		FileUtils.writeStringToFile(new File("model.as-string"), classifier.getClassifier.asInstanceOf[RandomForest].toString)
		System.exit(1)
	}
	*/

}
