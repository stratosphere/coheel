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
	 * @param id An auto-incrementing id for a link. Note: This field alone is no key, because ids are generated at each node.
	 *           Therefore, only (id, source) makes a key. Therefore, see the fullId method, which returns an unique string id.
	 */
	// Note: In contrast to InternalLink, this class does not contain a Node, because
	// that should not be part of the interface of this class.
	case class Link(surface: String, surfaceRepr: String, posTags: Vector[String], source: String, destination: String, id: Int = newId()) {
		def fullId: String = s"$id-${Util.id(source)}-${Util.id(surface)}"
	}

	/**
	 * Classifiable keeps track of the feature data
	 *
	 * It is just an abstraction to use slightly different code at train and classification time
	 * For example, at classification time, we need to keep track of the trie hit information, at training time we need to keep track of the gold standard
	 */
	case class Classifiable[T <: Info](id: String, surfaceRepr: String, context: Array[String], candidateEntity: String = "", surfaceProb: Double = -1.0, contextProb: Double = -1.0, info: T)

	abstract class Info {
		def stringInfo: List[String]
		def extraFeatures(classifiable: Classifiable[_]): List[Double]
	}

	case class TrainInfo(source: String, destination: String, posTags: Array[Double]) extends Info {
		override def stringInfo = List(source)
		override def extraFeatures(classifiable: Classifiable[_]): List[Double] = {
			posTags.toList ::: List(if (destination == classifiable.candidateEntity) 1.0 else 0.0)
		}
	}
	case class ClassificationInfo(trieHit: TrieHit, posTags: Array[Double]) extends Info {
		override def stringInfo: List[String] = List()
		override def extraFeatures(classifiable: Classifiable[_]): List[Double] = List()
	}



//	case class LinkWithContext(fullId: String, surfaceRepr: String, source: String, destination: String, context: Array[String], posTags: Array[Double])
	case class WordInDocument(document: String, word: String, count: Int)
	case class LanguageModel(pageTitle: String, model: Map[String, Double])
	case class WordCounts(word: WordInDocument, count: Int)

	case class LinkCandidate(fullId: String, surfaceRepr: String, source: String, destination: String, candidateEntity: String, prob: Double, context: Array[String], posTagsScores: Array[Int])

	case class LinkWithScores(fullId: String, surfaceRepr: String, source: String, destination: String, candidateEntity: String, posTagScores: Array[Double], promScore: Double, contextScore: Double)
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
	case class Plaintext(pageTitle: String, plainText: String, linkString: String)

	// Redirect resolving
	case class ContextLinkWithOrig(from: String, origTo: String, to: String, prob: Double)
	case class Redirect(from: String, to: String)

	// Training
	case class TrainingData(fullId: String, surfaceRepr: String, source: String, candidateEntity: String, features: Array[Double])
	case class FeatureLine(stringInfo: Seq[String], features: Seq[Double])

	// Classification
	case class InputDocument(id: String, tokens: mutable.ArrayBuffer[String], tags: mutable.ArrayBuffer[String])
	case class SurfaceProb(surface: String, destination: String, prob: Double)

	var currentId = 0
	def newId(): Int = {
		currentId += 1
		currentId
	}
}
