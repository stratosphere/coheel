package de.uni_potsdam.hpi.coheel.programs

import java.lang.Iterable

import de.uni_potsdam.hpi.coheel.Params
import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.ml.CoheelClassifier.POS_TAG_GROUPS
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import de.uni_potsdam.hpi.coheel.util.Util
import de.uni_potsdam.hpi.coheel.wiki.FullInfoWikiPage
import org.apache.flink.api.common.functions.{BroadcastVariableInitializer, RichGroupReduceFunction}
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import scala.collection.mutable


class TrainingDataProgram extends NoParamCoheelProgram with Serializable {

	val SAMPLE_FRACTION = if (runsOffline()) 100 else 5000
	val SAMPLE_NUMBER = if (runsOffline()) 0 else 3786

	override def getDescription = "Wikipedia Extraction: Build training data"


	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val wikiPages = readWikiPagesWithFullInfo { pageTitle =>
			Math.abs(pageTitle.hashCode) % SAMPLE_FRACTION == SAMPLE_NUMBER
		}

		wikiPages
			.map { wikiPage => wikiPage.pageTitle }
			.writeAsText(trainingDataPagesPath + s"-$SAMPLE_NUMBER.wiki", FileSystem.WriteMode.OVERWRITE)

		val linkDestinationsPerEntity = wikiPages.map { wp =>
			LinkDestinations(wp.pageTitle, wp.links.values.map { l =>
				l.destination
			}.toSeq)
		}

		val classifiables = wikiPages
			.flatMap(new LinksAsTrainingDataFlatMap(params))
			.name("Links and possible links")

		classifiables.map { c =>
			(c.id, c.surfaceRepr, c.info, c.info.posTags.deep, c.context.deep)
		}.writeAsTsv(trainingDataClassifiablesPath +  s"-$SAMPLE_NUMBER.wiki")

		// Fill classifiables with candidates, surface probs and context probs
		val featuresPerGroup = FeatureHelper.buildFeaturesPerGroup(env, classifiables)

		val trainingData = featuresPerGroup
			.reduceGroup(new TrainingDataGroupReduce)
			.withBroadcastSet(linkDestinationsPerEntity, TrainingDataGroupReduce.BROADCAST_LINK_DESTINATIONS_PER_ENTITY)
			.name("Training Data")

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

object TrainingDataGroupReduce {
	val BROADCAST_LINK_DESTINATIONS_PER_ENTITY = "linkDestinationsPerEntity"
}
class TrainingDataGroupReduce extends RichGroupReduceFunction[Classifiable[TrainInfo], String] {
	import CoheelLogger._
	var linkDestinationsPerEntity: mutable.Map[String, Seq[String]] = _
	override def open(params: Configuration): Unit = {
		linkDestinationsPerEntity = getRuntimeContext.getBroadcastVariableWithInitializer(
			TrainingDataGroupReduce.BROADCAST_LINK_DESTINATIONS_PER_ENTITY,
			new LinkDestinationsInitializer)
	}

	/**
	  * @param candidatesIt All link candidates with scores (all Classifiable's have the same id).
	  */
	override def reduce(candidatesIt: Iterable[Classifiable[TrainInfo]], out: Collector[String]): Unit = {
		val allCandidates = candidatesIt.asScala.toSeq

		// get all the link destinations from the source entitity of this classifiable
		// remember, all classifiables come from the same link/trie hit, hence it is ok to
		// only access the head
		val linkDestinations = if (allCandidates.head.id.startsWith("TH-"))
				linkDestinationsPerEntity(allCandidates.head.info.source)
			else
				null
		FeatureHelper.applyCoheelFunctions(allCandidates) { featureLine =>
			// What's missing: How to know all the links (entities) of the source entity, to filter
			// the bad candidates out here
			// The filtering must be done here, after the second order functions have been run

			import featureLine._
			def stringInfo = List(id, surfaceRepr, candidateEntity) ::: featureLine.info.modelInfo
			val output = s"${stringInfo.mkString("\t")}\t${featureLine.features.mkString("\t")}"

			// Filter out feature lines with a candidate entity, which is also a link in the source.
			// Taking care, that not all links are filtered out (not the original), i.e. only do this for trie hits
			if (id.startsWith("TH-")) {
				// This classifiable/feature line came from a trie hit, we might want to remove it from the
				// training data set:
				// Remove the trie hit, if the candidate entity is linked from the current article.
				// Reasoning: Say, an article contains Angela Merkel as a link. Later, it is referred to as
				// the "merkel" with no link. It would be wrong to learn, that this should not be linked, because
				// it is probably only not linked, because it was already linked in the article.
				if (!linkDestinations.contains(featureLine.candidateEntity))
					out.collect(output)
				else {
					log.info(s"Not output surface `${featureLine.surfaceRepr}` with candidate '${featureLine.candidateEntity}' from ${featureLine.info.modelInfo}")
				}
			} else {
				// we have a link, just output it
				out.collect(output)
			}
		}
	}
}

class LinksAsTrainingDataFlatMap(params: Params) extends ReadTrieFromDiskFlatMap[FullInfoWikiPage, Classifiable[TrainInfo]](params) {
	var trieHitCount: Int = 1
	import CoheelLogger._

	override def flatMap(wikiPage: FullInfoWikiPage, out: Collector[Classifiable[TrainInfo]]): Unit = {
		val linksWithPositions = wikiPage.links

		trie.findAllInWithTrieHit(wikiPage.plainText).foreach { trieHit =>
			// TODO: Do not output trie hits from links: This needs testing.
			if (!linksWithPositions.contains(trieHit.startIndex)) {
				val contextOption = Util.extractContext(wikiPage.plainText, trieHit.startIndex)

				contextOption.foreach { context =>
					val tags = wikiPage.tags.slice(trieHit.startIndex, trieHit.startIndex + trieHit.length).toArray
					out.collect(Classifiable[TrainInfo](
						// TH for trie hit
						s"TH-${Util.id(wikiPage.pageTitle)}-$trieHitCount",
						trieHit.s,
						context.toArray,
						surfaceLinkProb = trieHit.prob,
						info = TrainInfo(wikiPage.pageTitle, destination = "", POS_TAG_GROUPS.map { group => if (group.exists(tags.contains(_))) 1.0 else 0.0 })))
					trieHitCount += 1
				}
			} else {
				log.info(s"Ignoring trie hit $trieHit because it stems from link ${linksWithPositions(trieHit.startIndex)}")
			}
		}

		assert(wikiPage.tags.size == wikiPage.plainText.size)
		linksWithPositions.foreach { case (index, link) =>
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

			val containsResult = trie.contains(link.surfaceRepr)
			if (containsResult.asEntry) {
				contextOption.foreach { context =>
					out.collect(
						Classifiable[TrainInfo](
							link.id,
							link.surfaceRepr,
							context.toArray,
							surfaceLinkProb = containsResult.prob,
							info = TrainInfo(link.source, link.destination, POS_TAG_GROUPS.map { group => if (group.exists(link.posTags.contains(_))) 1.0 else 0.0 })))
				}
			}
		}
	}
}

