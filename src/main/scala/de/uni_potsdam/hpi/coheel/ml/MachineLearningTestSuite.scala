package de.uni_potsdam.hpi.coheel.ml

import java.io.{FileOutputStream, ObjectOutputStream, File}

import de.uni_potsdam.hpi.coheel.util.Timer
import org.apache.commons.io.FileUtils
import weka.classifiers.bayes.NaiveBayes
import weka.classifiers.{CostMatrix, Evaluation}
import weka.classifiers.functions.{Logistic, MultilayerPerceptron, SMO, SimpleLogistic}
import weka.classifiers.meta.CostSensitiveClassifier
import weka.classifiers.trees.{J48, RandomForest}
import weka.core._
import weka.filters.Filter
import weka.filters.unsupervised.attribute.Remove

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.{Success, Failure, Try, Random}


object MachineLearningTestSuite {

	val CLASS_INDEX = 16


	val removeFilter = {
		val remove = new Remove
		remove.setAttributeIndices("1")
		remove
	}

	def main(args: Array[String]) = {
		println("Reading.")
		val r = new Random(21011991)
		val instanceGroups = readInstancesInGroups()
		val groupsCount = instanceGroups.size
		val trainingRatio = (groupsCount * 0.7).toInt
		println(s"There are $groupsCount instance groups.")

		val randomOrder = r.shuffle(instanceGroups)

		println("Building separate training and validation set.")
		val training = randomOrder.take(trainingRatio)
		val fullTrainingInstances = buildInstances("train-full", training.flatten)
		removeFilter.setInputFormat(fullTrainingInstances)
		val test = randomOrder.drop(trainingRatio)
		val fullTestInstances     = buildInstances("test-full", test.flatten)

//		serializeGoodClassifier(fullTrainingInstances)

		println("Use all instances")
		println("=" * 80)
		runWithInstances(fullTrainingInstances, fullTestInstances)

		val oneSampleTrainingInstances = buildInstances("train-one",
			randomOrder.take(trainingRatio).map { group =>
//				if (!group.exists { inst => inst.value(CLASS_INDEX) == 1.0 }) {
//					group.foreach(println)
//					System.exit(10)
//				}
				val positive = group.find { inst => inst.value(CLASS_INDEX) == 1.0 }.headOption
				val negatives = group.filter { inst => inst.value(CLASS_INDEX) == 0.0 }
				val negative = r.shuffle(negatives).headOption
				positive.toBuffer ++ negative.toBuffer
			}.flatten
		)
		println("Use only one negative example")
		println("=" * 80)
		runWithInstances(oneSampleTrainingInstances, fullTestInstances)

		println("#" * 80)
		println("#" * 80)
		println("#" * 80)

		println("Use all instances")
		println("=" * 80)
		runGroupWise(fullTrainingInstances, test)
		println("Use only one negative example")
		println("=" * 80)
		runGroupWise(oneSampleTrainingInstances, test)

		// missing values

		// surface-link-at-all probability?
		// context < 100 ==>  Missing value
	}

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

	def buildInstance(split: Array[String]): Instance = {
		val attValues = split.map(_.toDouble).array
		val instance = new Instance(1.0, attValues)
		instance
	}

	def readInstancesInGroups(): ArrayBuffer[ArrayBuffer[Instance]] = {
		val scoresFile = new File("cluster-output/raw-training-data.tsv")
		val scoresSource = Source.fromFile(scoresFile)
		val groups = ArrayBuffer[ArrayBuffer[Instance]]()
		var currentGroup = ArrayBuffer[Instance]()
		var lastId: String = ""
		val lines = scoresSource.getLines()
		lines.drop(1)
		var lineNr = 1
		lines.foreach { line =>
			val split = line.split("\t")
			val id = split.head
			if (id != lastId && currentGroup.nonEmpty) {
				groups += currentGroup.clone()
				currentGroup.clear()
				lastId = id
			}
			split(0) = lineNr.toString
			lineNr += 1
			currentGroup += buildInstance(split)
		}
		groups
	}

	def runWithInstances(training: Instances, test: Instances): Unit = {
		val filteredTraining = Filter.useFilter(training, removeFilter)

		classifiers.foreach { case (name, classifier) =>
			val runtimeTry = Try(Timer.timeFunction {
				classifier.buildClassifier(filteredTraining)
			})
			runtimeTry match {
				case Failure(e) =>
					println(s"$name failed with ${e.getMessage}")
				case Success(runtime) =>
					println(s"$name in ${msToMin(runtime.toInt)} min")
					val evaluation = new Evaluation(filteredTraining)
					val time = Timer.timeFunction {
						classifier.buildClassifier(filteredTraining)
						evaluation.evaluateModel(classifier, Filter.useFilter(test, removeFilter))

					}
					System.out.println(f"P: ${evaluation.precision(1)}%.3f, R: ${evaluation.recall(1)}%.3f in ${msToMin(time.toInt)} min")
			}
		}
		println("-" * 80)
	}

	def msToMin(t: Int): Int = {
		t / (1000 * 60)
	}

	def runGroupWise(training: Instances, test: ArrayBuffer[ArrayBuffer[Instance]]) =  {
		val filteredTraining = Filter.useFilter(training, removeFilter)

		classifiers.foreach { case (name, classifier) =>
			val runtimeTry = Try(Timer.timeFunction {
				classifier.buildClassifier(filteredTraining)
			})
			runtimeTry match {
				case Failure(e) =>
					println(s"$name failed with ${e.getMessage}")
				case Success(runtime) =>
					println(s"$name in ${msToMin(runtime.toInt)} min")
					var tp = 0
					var fp = 0
					var fn = 0
					var tn = 0
					val time = Timer.timeFunction {
						test.foreach { group =>
							var positiveCount = 0
							var truePositiveCount = 0
							var trueCount = 0

							group.foreach { instance =>
								val filteredInstance = {
									removeFilter.input(instance); removeFilter.batchFinished(); removeFilter.output()
								}
								val pred = classifier.classifyInstance(filteredInstance)
								val act = filteredInstance.classValue()
								assert(act == filteredInstance.value(15))
								if (pred == 1.0) {
									positiveCount += 1
									if (act == 1.0)
										truePositiveCount += 1
								}
								if (act == 1.0)
									trueCount += 1
							}

							// we found the one correct
							if (positiveCount == 1 && truePositiveCount == 1)
								tp += 1
							// there is no true positive, but we predicted one
							else if (positiveCount >= 1 && truePositiveCount == 0)
								fp += 1
							// there is a true positive, but we predicted no one or more than one
							else if (positiveCount != 1 && trueCount == 1)
								fn += 1
							// there is none and we find none
							else if (positiveCount == 0 && trueCount == 0)
								tn += 1
							else {
								println("ERROR, printing all the instances")
								group.foreach(println)
								throw new RuntimeException(s"Uncovered case! positiveCount = $positiveCount, trueCount = $trueCount, truePositiveCount = $truePositiveCount")
							}
						}
					}
					val precision = tp.toDouble / (tp + fp)
					val recall    = tp.toDouble / (tp + fn)
					System.out.println(f"P: $precision%.3f, R: $recall%.3f in ${msToMin(time.toInt)} min")
			}
			println("-" * 80)
		}
	}


	def classifiers = {
		println("----------- Rebuilding classifiers")
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
		val instances = new Instances(name, CoheelClassifier.FEATURE_DEFINITION, 10000)
		instanceSeq.foreach { inst =>
			instances.add(inst)
		}
		instances.setClassIndex(CLASS_INDEX)
		instances
	}
}
