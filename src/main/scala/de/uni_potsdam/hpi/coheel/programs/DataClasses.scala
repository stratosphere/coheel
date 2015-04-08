package de.uni_potsdam.hpi.coheel.programs

object DataClasses {

	/**
	 * Represents a link in a Wikipedia article.
	 * @param surface The link's text, e.g. 'Merkel'
	 * @param surfaceRepr The representation of the surface as needed for the application. In contrast to <code>surface</code>,
	 *      which contains the raw link text, this representation can be stemmed, tokenized, or what's necessary for the
	 *      application.
	 * @param source The page the link is on, e.g. 'Germany'
	 * @param destination The link's destination, e.g. 'Angela Merkel'
	 */
	// Note: In contrast to InternalLink, this class does not contain a Node, because
	// that should not be part of the interface of this class.
	case class Link(surface: String, surfaceRepr: String, source: String, destination: String)
	case class LinkWithContext(link: Link, context: Array[String])
	case class WordInDocument(document: String, word: String, count: Int)
	case class LanguageModel(pageTitle: String, model: Map[String, Double])
	case class WordCounts(word: WordInDocument, count: Int)
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

	// Classification
	case class SurfaceProb(surface: Array[String], destination: String, prob: Double)
}
