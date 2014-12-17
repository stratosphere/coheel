package de.uni_potsdam.hpi.coheel.programs

import scala.collection.mutable

object DataClasses {

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

	//
	case class SurfaceProbLink(surface: Array[String], destination: String, prob: Double)
}
