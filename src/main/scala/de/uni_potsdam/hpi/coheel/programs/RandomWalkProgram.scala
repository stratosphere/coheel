package de.uni_potsdam.hpi.coheel.programs

import java.io.File
import java.net.URI

import de.uni_potsdam.hpi.coheel.datastructures.TrieHit
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.{FileSystem, Path}

class RandomWalkProgram extends CoheelProgram[Int] with Serializable {

	import CoheelLogger._

	override def getDescription: String = "CohEEL RandomWalk"

	// Select, which neighbours file to use
	val NEIGHBOURS_FILE = fullNeighboursPath
	//	val NEIGHBOURS_FILE = reciprocalNeighboursPath

	val neighboursCreationMethod = Map(fullNeighboursPath -> buildFullNeighbours _, reciprocalNeighboursPath -> buildReciprocalNeighbours _)

	def arguments = List(10, 100, 1001, 10000)

	override def buildProgram(env: ExecutionEnvironment, nrDocuments: Int): Unit = {
		val basicClassifierResults = env.readTextFile(classificationPath.replace(".wiki", s".$nrDocuments.wiki")).map { line =>
			val split = line.split("\t")
			val classificationType = if (split(1) == "SEED")
				NodeTypes.SEED
			else if (split(1) == "CANDIDATE")
				NodeTypes.CANDIDATE
			else
				throw new RuntimeException(s"Unknown classification type ${split(1)}")
			val trieHit = {
				val trieHitSplit = split(3).substring(0, split(3).length - 1).replace("TrieHit(", "").split(",")
				TrieHit(trieHitSplit(0), trieHitSplit(1).toFloat, trieHitSplit(2).toInt, trieHitSplit(3).toInt)
			}
			ClassifierResult(split(0), classificationType, split(1), trieHit)
		}

		val fileExists = if (runsOffline())
			new File(NEIGHBOURS_FILE).exists()
		else {
			val f = FileSystem.get(new URI("hdfs://tenemhead2"))
			f.exists(new Path(NEIGHBOURS_FILE.replace("hdfs://tenemhead2", "")))
		}

		val preprocessedNeighbours = if (fileExists) {
			log.info("Neighbourhood-File exists")
			loadNeighboursFromDisk(env, NEIGHBOURS_FILE)
		} else {
			log.info("Neighbourhood-File does not exist")
			neighboursCreationMethod(NEIGHBOURS_FILE)(env)
		}

		val withNeighbours = basicClassifierResults.join(preprocessedNeighbours)
			.where("candidateEntity")
			.equalTo("entity")
			.name("Join With Neighbours")
			.map { joinResult => joinResult match {
				case (classifierResult, neighbours) =>
					ClassifierResultWithNeighbours(
						classifierResult.documentId,
						classifierResult.classificationType,
						classifierResult.candidateEntity,
						classifierResult.trieHit,
						neighbours.in,
						neighbours.out)
			}
			}

		/*
		 * OUTPUT
		 */
		if (!fileExists) {
			preprocessedNeighbours.map(serializeNeighboursToString _).name("Serialized Neighbours").writeAsText(NEIGHBOURS_FILE, FileSystem.WriteMode.OVERWRITE)
		}

		// Write candidate classifier results for debugging
		basicClassifierResults.map { res =>
			(res.documentId, res.classificationType, res.candidateEntity, res.trieHit)
		}.name("Classifier-Results").writeAsTsv(classificationPath.replace(".wiki", "2.wiki"))

		withNeighbours.groupBy("documentId").reduceGroup(new RandomWalkReduceGroup).name("Random Walk").writeAsTsv(randomWalkResultsPath.replace(".wiki", s".$nrDocuments.wiki"))

	}

	def buildReciprocalNeighbours(env: ExecutionEnvironment): DataSet[Neighbours] = {
		val fullNeighbours = buildFullNeighbours(env)
		fullNeighbours.map { neighbours =>
			import neighbours._
			val inSet  = in.map(_.entity).toSet
			val outSet = out.map(_.entity).toSet
			val intersection = inSet.intersect(outSet)

			val newIn = in.filter { x => intersection.contains(x.entity) }
			val inSum = newIn.map(_.prob).sum
			newIn += Neighbour(RandomWalkReduceGroup.NULL_NODE, 1.0 - inSum)

			val newOut = out.filter { x => intersection.contains(x.entity) }
			val outSum = newOut.map(_.prob).sum
			newOut += Neighbour(RandomWalkReduceGroup.NULL_NODE, 1.0 - outSum)

			Neighbours(entity, newIn, newOut)
		}
	}

	def buildFullNeighbours(env: ExecutionEnvironment): DataSet[Neighbours] = {
		val contextLinks = env.readTextFile(contextLinkProbsPath).name("ContextLinkProbs-Path").map { line =>
			val split = line.split('\t')
			ContextLink(split(0), split(1), split(2).toDouble)
		}.name("ContextLinks")
		val outgoingNeighbours = contextLinks.groupBy("from").reduceGroup { grouped =>
			val asList = grouped.toBuffer
			(asList.head.from, asList.map { contextLink => Neighbour(contextLink.to, contextLink.prob) })
		}.name("Outgoing Neighbours")
		val incomingNeighbours = contextLinks.groupBy("to").reduceGroup { grouped =>
			val asList = grouped.toBuffer
			(asList.head.to, asList.map { contextLink => Neighbour(contextLink.from, contextLink.prob) })
		}.name("Incoming Neighbours")
		val fullNeighbours = incomingNeighbours.join(outgoingNeighbours)
			.where(0)
			.equalTo(0)
			.map { joinResult => joinResult match {
				case (in, out) => Neighbours(in._1, in._2, out._2)
			}
			}.name("All-Neighbours")
		fullNeighbours
	}


	def serializeNeighboursToString(neighbours: Neighbours): String = {
		val inString = neighbours.in.map { n => s"${n.entity}\0${n.prob}" }.mkString("\0")
		val outString = neighbours.out.map { n => s"${n.entity}\0${n.prob}" }.mkString("\0")
		s"${neighbours.entity}\t$inString\t$outString"
	}
	def loadNeighboursFromDisk(env: ExecutionEnvironment, neighboursPath: String): DataSet[Neighbours] = {
		env.readTextFile(neighboursPath).map { neighboursLine =>
			val Array(entity, inString, outString) = neighboursLine.split('\t')
			val inNeighbours = inString.split("\0").grouped(2).map { case Array(ent, prob) => Neighbour(ent, prob.toDouble) }.toBuffer
			val outNeighbours = outString.split("\0").grouped(2).map { case Array(ent, prob) => Neighbour(ent, prob.toDouble) }.toBuffer
			Neighbours(entity, inNeighbours, outNeighbours)
		}.name("All-Neighbours")
	}
}
