package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.datastructures.TrieHit
import de.uni_potsdam.hpi.coheel.util.Util
import scala.collection.mutable

object DataClasses {

	/**
	 * Represents a link in a Wikipedia article.
	 * @param surface The link's text, e.g. 'Merkel'
	 * @param surfaceRepr The representation of the surface as needed for the application. In contrast to <code>surface</code>,
	 *      which contains the raw link text, this representation can be stemmed, tokenized, or what's necessary for the
	 *      application.
	 * @param source The page the link is on, e.g. 'Germany'
	 * @param destination The link's destination, e.g. 'Angela Merkel'
	 * @param pos Position of the link in the text. Use for generating a unique id.
	  *           See the fullId method, which returns an unique string id.
	 */
	// Note: In contrast to InternalLink, this class does not contain a Node, because
	// that should not be part of the interface of this class.
	case class Link(surface: String, surfaceRepr: String, posTags: Vector[String], source: String, destination: String, pos: Int) {
		def id: String = f"L-${Util.id(source)}-$pos%08d-${Util.id(surface)}"
	}

	case class WordInDocument(document: String, word: String, count: Int)
	case class LanguageModel(pageTitle: String, model: Map[String, Int]) {

		// smoothing factor
		val alpha = 0.1
		// number of words in the current model
		val wordsInModel = model.values.sum
		// number of distinct words in the entire corpus
		val N = 17964411
		val denominator = wordsInModel + alpha * N

		def prob(word: String): Double = {
			model.get(word) match {
				case Some(count) =>
					(count + alpha) / denominator
				case None =>
					alpha / denominator
			}
		}
	}
	case class WordCounts(word: WordInDocument, count: Int)

	case class LinkContextScore(id: Int, surfaceRepr: String, contextProb: Double)
	case class DocumentCounts(document: String, count: Int)
	case class SurfaceCounts(surfaceRepr: String, count: Int)
	case class SurfaceLinkCounts(surface: String, destination: String, count: Int)
	case class LinkCounts(source: String, count: Int)
	case class ContextLinkCounts(source: String, destination: String, count: Int)
	case class ContextLink(from: String, to: String, prob: Double)

	// NER
	case class EntireTextSurfaces(pageTitle: String, surface: String)
	case class EntireTextSurfaceCounts(surface: String, count: Int)
	case class SurfaceAsLinkCount(surface: String, count: Int)

	// Surface Evaluation
	case class PlainText(pageTitle: String, plainText: String, linkString: String)

	// Redirect resolving
	trait ThingToResolve[T] {
		def to: String
		def updateTo(s: String): T
	}
	case class ContextLinkCountsResolving(from: String, to: String, count: Int) extends ThingToResolve[ContextLinkCountsResolving] {
		override def updateTo(s: String): ContextLinkCountsResolving = this.copy(to = s)
	}
	case class SurfaceLinkCountsResolving(surface: String, destination: String, count: Int) extends ThingToResolve[SurfaceLinkCountsResolving] {
		override def to: String = destination
		override def updateTo(s: String): SurfaceLinkCountsResolving = this.copy(destination = s)
	}
	case class Redirect(from: String, to: String)

	// Training

	/**
	 * Classifiable keeps track of the surface and context of an instance.
	 * Furthermore, by a generic info element it tracks extra information about the instance.
	 * An instance may also add further features (see below), e.g. pos tags, which are not directly linked to the second order functions.
	 *
	 * It is just an abstraction to use slightly different code at train and classification time
	 * For example, at classification time, we need to keep track of the trie hit information, at training time we need to keep track of the gold standard.
	 * This abstraction is done with an Info object, see below.
	 */
	case class Classifiable[T <: Info](
			id: String,
			surfaceRepr: String,
			context: Array[String],
			candidateEntity: String = "",
			surfaceProb: Double = -1.0,
			surfaceLinkProb: Double,
			contextProb: Double = -1.0,
			info: T) {

		def withCandidateEntityAndSurfaceProb(newCandidateEntity: String, newSurfaceProb: Double): Classifiable[T] = {
			Classifiable[T](id, surfaceRepr, context, newCandidateEntity, newSurfaceProb, surfaceLinkProb, contextProb, info)
		}

		def withContextProb(newContextProb: Double): Classifiable[T] = {
			Classifiable[T](id, surfaceRepr, context, candidateEntity, surfaceProb, surfaceLinkProb, newContextProb, info)
		}
	}

	case class LinkDestinations(entity: String, destinations: Seq[String])


	/**
	 * Tracks extra information, both about the model of an instance and about further features, which are not directly linked to second order functions.
	 */
	abstract class Info {
		// Maybe move further features from Info to extra class? It's not really extra info?
		def furtherFeatures(classifiable: Classifiable[_]): List[Double]
	}

	case class TrainInfo(source: String, destination: String, posTags: Array[Double]) extends Info {
		def modelInfo: List[String] = List(source, destination)
		override def furtherFeatures(classifiable: Classifiable[_]): List[Double] = {
			posTags.toList ::: List(if (destination == classifiable.candidateEntity) 1.0 else 0.0)
		}
	}
	case class ClassificationInfo(documentId: String, trieHit: TrieHit, posTags: Array[Double]) extends Info {
		override def furtherFeatures(classifiable: Classifiable[_]): List[Double] = posTags.toList
	}

	object ClassificationType extends Enumeration {
		type ClassificationType = Value
		val SEED, CANDIDATE = Value
	}


	case class TrainingData(fullId: String, surfaceRepr: String, source: String, candidateEntity: String, features: Array[Double])

	/**
	 * Keeps track of the features of an instance, and it's accompanying real-world information.
	 * FeatureLine's are passed to the classifier, model can be used to identify which element was classified.
	 */
	case class FeatureLine[T <: Info](id: String, surfaceRepr: String, candidateEntity: String, info: T, features: Seq[Double])

	// Classification
	/**
	 * Input Documents are used to represent the input to the classification. If the same document needs to be
	 * distributed to multiple nodes, then we have several input documents.
	 * @param id Unique id of the document
	 * @param replication Tells the replication number of this document. If we need to distribute the input text to
	 *                    two different machines, we will create two InputDocuments. The first will have the replication
	 *                    number 0, the second the replication number 1.
	 * @param index Where to distribute this document to. This number must be between 0 and number of nodes - 1
	 * @param tokens The tokens in this document.
	 * @param tags The corresponding tags. This Seq has the same length as the tokens Seq.
	 */
	case class InputDocument(id: String, replication: Int, index: Int, tokens: mutable.ArrayBuffer[String], tags: mutable.ArrayBuffer[String])
	case class SurfaceProb(surface: String, destination: String, prob: Double)

	case class Neighbour(entity: String, prob: Double)

	case class Neighbours(entity: String, in: Seq[Neighbour], out: Seq[Neighbour])

	/**
	 * All candidates of one document.
	 */
	case class DocumentCandidateGroup(seeds: Seq[FeatureLine[ClassificationInfo]], candidates: Seq[FeatureLine[ClassificationInfo]])

	/**
	 * @param documentId Document this classification came from.
	 * @param classifierType Either "seed" or "candidate"
	 * @param candidateEntity Candidate entity to link
	 */
	case class ClassifierResult(documentId: String, classifierType: NodeType, candidateEntity: String, trieHit: TrieHit)

	case class ClassifierResultWithNeighbours(
		 documentId: String,
		 var classifierType: NodeType,
		 candidateEntity: String,
		 trieHit: TrieHit,
		 var in: Seq[Neighbour],
		 var out: Seq[Neighbour]) {

		def shortToString(): String = {
			s"($classifierType, '$candidateEntity', $trieHit)"
		}
	}

	object ClassifierResultWithNeighbours {
		def apply(documentId: String, classifierType: NodeType, candidateEntity: String, in: Seq[Neighbour], out: Seq[Neighbour]): ClassifierResultWithNeighbours =
			ClassifierResultWithNeighbours(documentId, classifierType, candidateEntity, null, in, out)
	}

	var currentId = 0
	def newId(): Int = {
		currentId += 1
		currentId
	}

	// My take at manually building enums
	// Using Scala enums (1) is ugly, (2) actually did not workout because of serialization issues; apparently enums are not simple enums in Scala
	trait NodeType
	object NodeTypes {
		object CANDIDATE extends NodeType { override def toString: String = "CANDIDATE" }
		object SEED extends NodeType { override def toString: String = "SEED" }
		object NEIGHBOUR extends NodeType { override def toString: String = "NEIGHBOUR" }
		object NULL extends NodeType { override def toString: String = "NULL" }
	}


	case class RandomWalkNode(entity: String) {

		var nodeType: NodeType = NodeTypes.NEIGHBOUR
		var visited: Boolean = false
		var isSink: Boolean = false

		def withNodeType(nodeType: NodeType) = {
			this.nodeType = nodeType
			this
		}
	}
}
