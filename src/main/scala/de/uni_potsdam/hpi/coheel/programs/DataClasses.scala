package de.uni_potsdam.hpi.coheel.programs

object DataClasses {

	case class Word(document: String, word: String)
	case class WordCounts(word: Word, count: Int)
	case class DocumentCounts(document: String, count: Int)
	case class SurfaceCounts(surface: String, count: Int)
	case class SurfaceLinkCounts(surface: String, destination: String, count: Int)
	case class LinkCounts(source: String, count: Int)
	case class ContextLinkCounts(source: String, destination: String, count: Int)

	case class EntireTextSurfaces(pageTitle: String, surface: String)
	case class EntireTextSurfaceCounts(surface: String, count: Int)
	case class SurfaceAsLinkCount(surface: String, count: Int)
}
