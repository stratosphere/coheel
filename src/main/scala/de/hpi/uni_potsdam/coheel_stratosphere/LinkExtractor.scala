package de.hpi.uni_potsdam.coheel_stratosphere

import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.InternalLinkNode
import org.dbpedia.extraction.wikiparser.Node
import org.dbpedia.extraction.wikiparser.impl.simple.SimpleWikiParser
import scala.collection.mutable

case class Link(node: Node, var text: String, var destination: String) {
	def this(node: Node) = this(node, null, null)
}

class LinkExtractor {

	var links: List[Link] = List()
	def extractLinks(wikiPage: WikiPage): List[Link] = {
		val wikiParser = new SimpleWikiParser()
		val ast = wikiParser.apply(wikiPage)
		walkAST(ast)
	}

	private def walkAST(parentNode: Node): List[Link] =  {
		links = List()
		val nodeQueue = mutable.Queue(parentNode)
		while (!nodeQueue.isEmpty) {
			val node = nodeQueue.dequeue()
			handleNode(node)
			nodeQueue.enqueue(node.children: _*)
		}
		links
	}

	private def handleNode(node: Node): Unit = {
		val link: Option[Link] = Some(new Link(node))
		link
			.flatMap(filterNonLinks)
			.flatMap(filterImages)
			.flatMap(filterFiles)
			.flatMap(filterCategories)
			.flatMap(removeInnerPageLinks)
			.foreach { link =>
				links = link :: links
			}
	}

	/**
	 * Handles links like Germany#History (link to a specific point in
	 * a page) and removes the part after '#'
	 * @return The sanitized link.
	 */
	def removeInnerPageLinks(link: Link): Option[Link] = {
		if (link.text == "")
			link.text = link.destination
		val hashTagIndex = link.text.indexOf("#")
		if (hashTagIndex != -1)
			link.text = link.text.substring(0, hashTagIndex)
		Some(link)
	}

	/**
	 * Filters out a wikiparser.Node, if it is not an internal link.
	 * @return Some(link), if it is a internal link, None otherwise.
	 */
	def filterNonLinks(link: Link): Option[Link] = {
		if (!link.node.isInstanceOf[InternalLinkNode])
			None
		else {
			val linkNode = link.node.asInstanceOf[InternalLinkNode]
			link.destination = linkNode.destination.decodedWithNamespace
			link.text = linkNode.toPlainText
			Some(link)
		}
	}

	/**
	 * Filters out a link, if it starts with a given string, e.g. 'Image:' or
	 * 'Category'.
	 * @param startString The string to check for.
	 * @return Some(link) if the link does not start with the given string,
	 *         None otherwise.
	 */
	def filterStartsWith(link: Link, startString: String): Option[Link] = {
		if (link.destination.startsWith(startString)) None
		else Some(link)
	}
	def filterImages(link: Link): Option[Link] = filterStartsWith(link, "Image:")
	def filterFiles(link: Link): Option[Link] = filterStartsWith(link, "File:")
	def filterCategories(link: Link): Option[Link] = filterStartsWith(link, "Category:")
}
