package de.uni_potsdam.hpi.coheel.programs

import java.lang.Iterable
import java.util.Date

import de.uni_potsdam.hpi.coheel.datastructures.NewTrie
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.ml.CoheelClassifier.POS_TAG_GROUPS
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import de.uni_potsdam.hpi.coheel.util.Util
import de.uni_potsdam.hpi.coheel.wiki.FullInfoWikiPage
import org.apache.flink.api.common.functions.{BroadcastVariableInitializer, RichGroupReduceFunction, RichFlatMapFunction}
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector
import org.apache.log4j.Logger
import scala.collection.JavaConverters._
import scala.collection.mutable


class TrainingDataProgram extends NoParamCoheelProgram with Serializable {

	val SAMPLE_FRACTION = if (runsOffline()) 100 else 5000
	val SAMPLE_NUMBER = if (runsOffline()) 0 else 3786

	override def getDescription = "Wikipedia Extraction: Build training data"

	def log: Logger = Logger.getLogger(getClass)

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val wikiPages = readWikiPagesWithFullInfo { pageTitle =>
			Math.abs(pageTitle.hashCode) % SAMPLE_FRACTION == SAMPLE_NUMBER
		}

		wikiPages.map { wikiPage => wikiPage.pageTitle }.writeAsText(trainingDataPagesPath + s"-$SAMPLE_NUMBER.wiki", FileSystem.WriteMode.OVERWRITE)

		val linkDestinationsPerEntity = wikiPages.map { wp =>
			LinkDestinations(wp.pageTitle, wp.links.values.map { l =>
				l.destination
			}.toSeq)
		}

		val classifiables = wikiPages
			.flatMap(new LinksAsTrainingDataFlatMap)
			.name("Links and possible links")

		classifiables.map { c =>
			(c.id, c.surfaceRepr, c.info, c.info.posTags.deep, c.context.deep)
		}.writeAsTsv(trainingDataClassifiablesPath +  s"-$SAMPLE_NUMBER.wiki")

		// Fill classifiables with candidates, surface probs and context probs
		val featuresPerGroup = FeatureHelper.buildFeaturesPerGroup(this, classifiables)

		val trainingData = featuresPerGroup
			.reduceGroup(new TrainingDataGroupedGroupReduce)
			.withBroadcastSet(linkDestinationsPerEntity, TrainingDataGroupedGroupReduce.BROADCAST_LINK_DESTINATIONS_PER_ENTITY)
			.name("Training Data")

		// TODO: Also join surface link probs
		trainingData.writeAsText(trainingDataPath + s"-$SAMPLE_NUMBER.wiki", FileSystem.WriteMode.OVERWRITE)
	}
}

class LinkDestinationsInitializer extends BroadcastVariableInitializer[LinkDestinations, mutable.Map[String, Seq[String]]] {

	override def initializeBroadcastVariable(destinations: Iterable[LinkDestinations]): mutable.Map[String, Seq[String]] = {
		val destinationsMap = mutable.Map[String, Seq[String]]()
		destinations.asScala.foreach { dest =>
			destinationsMap += dest.entity -> dest.destinations
		}
		destinationsMap
	}
}

object TrainingDataGroupedGroupReduce {
	val BROADCAST_LINK_DESTINATIONS_PER_ENTITY = "linkDestinationsPerEntity"
}
class TrainingDataGroupedGroupReduce extends RichGroupReduceFunction[Classifiable[TrainInfo], String] {
	def log = Logger.getLogger(getClass)
	var linkDestinationsPerEntity: mutable.Map[String, Seq[String]] = _
	override def open(params: Configuration): Unit = {
		linkDestinationsPerEntity = getRuntimeContext.getBroadcastVariableWithInitializer(
			TrainingDataGroupedGroupReduce.BROADCAST_LINK_DESTINATIONS_PER_ENTITY,
			new LinkDestinationsInitializer)
	}

	/**
	  * @param candidatesIt All link candidates with scores (all Classifiable's have the same id).
	  */
	override def reduce(candidatesIt: Iterable[Classifiable[TrainInfo]], out: Collector[String]): Unit = {
		val allCandidates = candidatesIt.asScala.toSeq
		FeatureProgramHelper.applyCoheelFunctions(allCandidates) { featureLine =>
			// TODO: This information is available
			featureLine.info.source
			featureLine.candidateEntity
			// What's missing: How to know all the links (entities) of the source entity, to filter
			// the bad candidates out here
			// The filtering must be done here, after the second order functions have been run

			import featureLine._
			def stringInfo = List(id, surfaceRepr, candidateEntity) ::: featureLine.info.modelInfo
			val output = s"${stringInfo.mkString("\t")}\t${featureLine.features.mkString("\t")}"
			// TODO: Here filter feature lines with a candidate entity, which is also a link in the source
			// TODO: Take care, that not all links are filtered out (not the original), i.e. only do this for trie hits
			out.collect(output)
		}
	}
}

class LinksAsTrainingDataFlatMap extends RichFlatMapFunction[FullInfoWikiPage, Classifiable[TrainInfo]] {
	var tokenHitCount: Int = 1

	override def flatMap(wikiPage: FullInfoWikiPage, out: Collector[Classifiable[TrainInfo]]): Unit = {
		assert(wikiPage.tags.size == wikiPage.plainText.size)
		wikiPage.links.foreach { case (index, link) =>
			// In theory, the index of the link should be in the set of indices proposed by the trie:
			//    assert(hitPoints.contains(index))
			// After all, if this link was found in the first phase, its surface should be in the trie now.
			// The problem, however, is the different tokenization: When tokenizing link text, we only tokenize
			// the small text of the link, while plain text tokenization works on the entire text
			// This tokenization is sometimes different, see the following example:
			//    println(TokenizerHelper.tokenize("Robert V.").mkString(" "))            --> robert v.
			//    println(TokenizerHelper.tokenize("Robert V. The Legacy").mkString(" ")) --> robert v the legaci (dot missing)
			// TODO: This could be solved by taking the link tokenization directly from the plain text, however, this would
			//       require quite a bit of rewriting.

			val contextOption = Util.extractContext(wikiPage.plainText, index)

			contextOption.foreach { context =>
				out.collect(Classifiable[TrainInfo](link.fullId, link.surfaceRepr, context.toArray, info = TrainInfo(link.source, link.destination, POS_TAG_GROUPS.map { group => if (group.exists(link.posTags.contains(_))) 1.0 else 0.0 })))
			}
		}


		// TODO: Should we also add wrong training instances, which are not linked at all?
//		trie.findAllInWithTrieHit(wikiPage.plainText).foreach { tokenHit =>
//			val context = Util.extractContext(wikiPage.plainText, tokenHit.startIndex)
//
//			context.foreach { textContext =>
//				val tags = wikiPage.tags.slice(tokenHit.startIndex, tokenHit.startIndex + tokenHit.length).toArray
//				// TH for trie hit
//				out.collect(LinkWithContext(s"TH-${Util.id(wikiPage.pageTitle)}-$tokenHitCount", tokenHit.s, wikiPage.pageTitle, destination = "", textContext.toArray, tags))
//				tokenHitCount += 1
//			}
//		}
	}
}

