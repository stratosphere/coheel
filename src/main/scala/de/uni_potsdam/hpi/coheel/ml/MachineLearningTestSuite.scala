package de.uni_potsdam.hpi.coheel.ml

import java.io.{FileWriter, File}

import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.programs.{CoheelLogger, CoheelProgram}
import de.uni_potsdam.hpi.coheel.programs.DataClasses.{ClassificationInfo, FeatureLine}
import de.uni_potsdam.hpi.coheel.util.Timer
import org.apache.commons.io.FileUtils
import weka.classifiers.CostMatrix
import weka.classifiers.bayes.NaiveBayes
import weka.classifiers.functions.{Logistic, MultilayerPerceptron, SimpleLogistic}
import weka.classifiers.meta.{SerialVersionAccess, CostSensitiveClassifier}
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
	extends DenseInstance(weight, attValues) {

	def this(other: CoheelInstance) = this(other.weight(), other.toDoubleArray, other.info)

	override def copy(): AnyRef = {
		val result = new CoheelInstance(this)
		result.m_Dataset = m_Dataset
		result
	}
}

object MachineLearningTestSuite {

	import CoheelLogger._

	val r = new Random(21011991)

	def main(args: Array[String]) = {
		val (train, test) = readTrainingDataAndBuildInstances()

//		serializeGoodClassifier(train)

		val expected = test.enumerateInstances().asScala.flatMap { case instance: CoheelInstance =>
			if (instance.classValue() == 1.0)
				// trie hit id, and correct entity
				Some((instance.info.id, instance.info.destination))
			else
				None
		}.toSet

		println("#" * 10 + " Test classifiers " + "#" * 10)
		testCoheelClassifiers(train, test, expected)
	}

	def readTrainingDataAndBuildInstances(): (Instances, Instances) = {
		print("Reading .. "); Console.flush()
		val trainSet = r.shuffle(readInstancesInGroups("632"))
		val testSet  = r.shuffle(readInstancesInGroups("3786"))
		println("Done.")
		println(s"There are ${trainSet.size} instance groups in training and ${testSet.size} in test")
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
		val attValues = split.slice(5, 5 + CoheelClassifier.NUMBER_OF_FEATURES + 1).map(_.toDouble)
		val trainingData = TrainingData(id, surface, candidateEntity, source, destination)

		val instance = new CoheelInstance(1.0, attValues, trainingData)
		instance
	}

	def readInstancesInGroups(suffix: String): mutable.ArrayBuffer[mutable.ArrayBuffer[Instance]] = {
		val groups = mutable.ArrayBuffer[mutable.ArrayBuffer[Instance]]()

		val scoresSource = Source.fromFile(new File(s"output/training-data-$suffix.wiki"))
		var currentGroup = mutable.ArrayBuffer[Instance]()
		var lastId: String = ""
		val lines = scoresSource.getLines()

		lines.foreach { line =>
			val split = line.split("\t")
			assert(split.length == 5 /* id, surface, candidate entity, source, destination */ + CoheelClassifier.NUMBER_OF_FEATURES + 1 /* class */)
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

	def testCoheelClassifiers(train: Instances, test: Instances, expected: Set[(String, String)]): Unit = {
		// ids of all links
		val expectedIds = expected.map( _._1)

		var i = 1
		classifiers.foreach { case (name, classifier) =>
			println(new java.util.Date)
			log.info(s"Starting training with ${FreeMemory.get(true)} MB of RAM")
			val runtimeTry = Try(Timer.timeFunction {
				classifier.buildClassifier(train)
			})
			log.info(s"Finished training with ${FreeMemory.get(true)} MB of RAM")
			runtimeTry match {
				case Failure(e) => println(s"    $name failed with ${e.getMessage}")
				case Success(trainingTime) =>
					println(s"    $name in ${msToMin(trainingTime.toInt)} min")

					val actualSeed = mutable.Set[(String, String)]()
					val actualCand = mutable.Set[(String, String)]()

					// collect instances in groups of same trie hit
					val instances = test.enumerateInstances().asScala.map { case instance: CoheelInstance => instance }.toList
					val instancesGroup = instances.groupBy { instance => instance.info.id }
					// build coheel classifier, which implements special group logic on top of classifier results
					val seedClassifier = new CoheelClassifier(classifier)
					val candidateClassifier = new CoheelClassifier(classifier)

					val fw = new FileWriter(s"no-link-seeds-$i.txt", true)
					val classificationTime = Timer.timeFunction {
						instancesGroup.values.foreach { group =>
							val featureLines = group.map { instance =>
								FeatureLine[ClassificationInfo](
									instance.info.id,
									instance.info.surface,
									instance.info.candidateEntity,
									ClassificationInfo(null, null, null),
									instance.attValues)
							}.toSeq
							candidateClassifier.classifyResultsWithCandidateLogic(featureLines).foreach { candidate =>
								actualCand.add((candidate.id, candidate.candidateEntity))
							}
							seedClassifier.classifyResultsWithSeedLogic(featureLines).foreach { seed =>
								// collect a result if there is one
								actualSeed.add((seed.id, seed.candidateEntity))
								if (!expectedIds.contains(seed.id)) {
									fw.write(s"$seed\n")
								}
							}
						}
					}
					fw.close()

					log.info(s"Finished prediction with ${FreeMemory.get(true)} MB of RAM")

					val precisionCand = if (actualCand.nonEmpty) expected.intersect(actualCand).size.toDouble / actualCand.size else -1.0
					val recallCand = expected.intersect(actualCand).size.toDouble / expected.size
					val f1Cand = 2 * precisionCand * recallCand / (precisionCand + recallCand)

					val precisionSeed = if (actualSeed.nonEmpty) expected.intersect(actualSeed).size.toDouble / actualSeed.size else -1.0
					val recallSeed = expected.intersect(actualSeed).size.toDouble / expected.size
					val f1Seed = 2 * precisionSeed * recallSeed / (precisionSeed + recallSeed)

					// only those seed classifications, which belong to links
					val (seedsAtLinks, seedsAtTrieHits) = actualSeed.partition { act => expectedIds.contains(act._1) }


					val precisionExpected = if (seedsAtLinks.nonEmpty) expected.intersect(seedsAtLinks).size.toDouble / seedsAtLinks.size else -1.0
					val recallExpected = expected.intersect(seedsAtLinks).size.toDouble / expected.size
					val f1Expected = 2 * precisionExpected * recallExpected / (precisionExpected + recallExpected)

					// how many seed/candidate classifications are actually links
					val precisionSeedsActualLinks = seedsAtLinks.size.toDouble / actualSeed.size
					val precisionCandidatesActualLinks = actualCand.count { act => expectedIds.contains(act._1) }.toDouble / actualCand.size
					val recallSeedsActualLinks = actualSeed.map(_._1).intersect(expectedIds).size.toDouble / expected.size
					val recallCandidatesActualLinks = actualCand.map(_._1).intersect(expectedIds).size.toDouble / expected.size

					println(s"      Classification Time: ${msToMin(classificationTime.toInt)} min")
					println(f"      P: $precisionCand%.3f, R: $recallCand%.3f, F1: $f1Cand%.3f (CANDIDATE)")
					println(f"      P: $precisionSeed%.3f, R: $recallSeed%.3f, F1: $f1Seed%.3f (SEED)")
					println(f"      P: $precisionExpected%.3f, R: $recallExpected%.3f, F1: $f1Expected%.3f (LINKS ONLY, SEED)")
					println(f"      Actual Links Precision: $precisionSeedsActualLinks%.3f (SEED), $precisionCandidatesActualLinks%.3f (CANDIDATE)")
					println(f"      Actual Links Recall: $recallSeedsActualLinks%.3f (SEED), $recallCandidatesActualLinks%.3f (CANDIDATE)")
					val fileName = s"model$i.model"
					SerializationHelper.write(fileName, classifier)
					println(s"      Written to disk as $fileName")
					i += 1
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
//			new Logistic,
//			() => new FastRandomForest,
//			() => new J48
			() => { val r = new RandomForest; r.setNumExecutionSlots(10); r }
//			new SimpleLogistic,
//			simpleLogistic,
//			new MultilayerPerceptron,
//			nb1,
//			nb2,
//			new SMO
		)
		base.flatMap { classifier =>
			val costMatrixFN = new CostMatrix(2)
			costMatrixFN.setElement(1, 0, 10)

			val costMatrixFP = new CostMatrix(2)
			costMatrixFP.setElement(0, 1, 10)

			val cost1 = new CostSensitiveClassifier
			cost1.setClassifier(classifier())
			cost1.setMinimizeExpectedCost(true)
			cost1.setCostMatrix(costMatrixFN)
			val cost2 = new CostSensitiveClassifier
			cost2.setClassifier(classifier())
			cost2.setCostMatrix(costMatrixFN)

			val cost3 = new CostSensitiveClassifier
			cost3.setClassifier(classifier())
			cost3.setMinimizeExpectedCost(true)
			cost3.setCostMatrix(costMatrixFP)
			val cost4 = new CostSensitiveClassifier
			cost4.setClassifier(classifier())
			cost4.setCostMatrix(costMatrixFP)

			val baseClassifier = classifier()
			val baseClassifierName = baseClassifier.getClass.getSimpleName
			List(
				(baseClassifierName, baseClassifier),
				(s"$baseClassifierName with 10 x FN cost, minimize expected cost = true", cost1),
//				(s"$baseClassifierName with 10 x FN cost, minimize expected cost = false", cost2),
				(s"$baseClassifierName with 10 x FP cost, minimize expected cost = true", cost3)
//				(s"$baseClassifierName with 10 x FP cost, minimize expected cost = false", cost4)
			)
		}
	}

	def buildInstances(name: String, instanceSeq: Seq[Instance]): Instances = {
		val instances = new Instances(name, CoheelClassifier.FEATURE_DEFINITION, instanceSeq.size)
		instanceSeq.foreach { inst =>
			instances.add(inst)
		}
		instances.setClassIndex(CoheelClassifier.NUMBER_OF_FEATURES)
		instances
	}

	def serializeGoodClassifier(train: Instances): Unit = {
		println("Serialize good classifier")
		println("=" * 80)
		// Build classifier
		val baseClassifier = new RandomForest
		// Apply costs
		val classifier = new CostSensitiveClassifier
		classifier.setClassifier(baseClassifier)
		classifier.setMinimizeExpectedCost(true)

		val costMatrixFP = new CostMatrix(2)
		costMatrixFP.setElement(0, 1, 10)
		classifier.setCostMatrix(costMatrixFP)

//		val costMatrixFN = new CostMatrix(2)
//		costMatrixFN.setElement(1, 0, 10)
//		classifier.setCostMatrix(costMatrixFN)
		// Train
		classifier.buildClassifier(train)
		// Serialize
		SerializationHelper.write("model.model", classifier)
		FileUtils.writeStringToFile(new File("model.as-string"), classifier.getClassifier.asInstanceOf[RandomForest].toString)
		System.exit(1)
	}

}
