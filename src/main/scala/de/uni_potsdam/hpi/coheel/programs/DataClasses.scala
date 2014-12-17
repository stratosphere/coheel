package de.uni_potsdam.hpi.coheel.programs

object DataClasses {

	/**
	 * Represents a link in a Wikipedia article.
	 * @param source The page the link is on, e.g. 'Germany'
	 * @param surface The link's text, e.g. 'Merkel'
	 * @param destination The link's destination, e.g. 'Angela Merkel'
	 */
	// Note: In contrast to InternalLink, this class does not contain a Node, because
	// that should not be part of the interface of this class.
	case class Link(source: String, surface: String, destination: String)

	case class WordInDocument(document: String, word: String, count: Int)
	case class WordCounts(word: WordInDocument, count: Int)
	case class DocumentCounts(document: String, count: Int)
	case class SurfaceCounts(surface: String, count: Int)
	case class SurfaceLinkCounts(surface: String, destination: String, count: Int)
	case class LinkCounts(source: String, count: Int)
	case class ContextLinkCounts(source: String, destination: String, count: Int)

	// NER
	case class EntireTextSurfaces(pageTitle: String, surface: String)
	case class EntireTextSurfaceCounts(surface: String, count: Int)
	case class SurfaceAsLinkCount(surface: String, count: Int)

	// Redirect resolving
	case class ContextLink(from: String, origTo: String, to: String, prob: Double)
	case class Redirect(from: String, to: String)

	// For classification
	case class SurfaceProbLink(surface: Array[String], destination: String, prob: Double)
}
