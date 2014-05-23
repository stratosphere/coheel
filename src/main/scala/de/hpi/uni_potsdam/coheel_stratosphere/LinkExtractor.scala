package de.hpi.uni_potsdam.coheel_stratosphere

import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.InternalLinkNode
import org.dbpedia.extraction.wikiparser.Node
import org.dbpedia.extraction.wikiparser.impl.simple.SimpleWikiParser
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class Link(node: Node, var text: String, var destination: String) {
	def this(node: Node) = this(node, null, null)
}

class LinkExtractor {

	var links: Seq[Link] = _
	def extractLinks(wikiPage: WikiPage): Seq[Link] = {
		val wikiParser = new SimpleWikiParser()
		val ast = wikiParser.apply(wikiPage)
		walkAST(ast)
	}

	private def walkAST(parentNode: Node): Seq[Link] =  {
		links = Vector()
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
//			.flatMap(debugPrintAllLinks)
			.flatMap(filterImages)
			.flatMap(filterFiles)
			.flatMap(filterCategories)
			.flatMap(removeAnchorLinks)
			.flatMap(filterExternalLinks)
			.foreach { link =>
				links = links :+ link
			}
	}

	/**
	 * Handles anchor links like Germany#History (link to a specific point in
	 * a page) and removes the part after '#'
	 * @return The sanitized link.
	 */
	def removeAnchorLinks(link: Link): Option[Link] = {
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
	def debugPrintAllLinks(link: Link): Option[Link] = {
		println(link.text + "#" + link.destination)
		Some(link)
	}

	/**
	 * Filters out a link, if it starts with a given string, e.g. 'Image:' or
	 * 'Category'.
	 * @param startStrings The string to check for.
	 * @return Some(link) if the link does not start with the given string,
	 *         None otherwise.
	 */
	def filterStartsWith(link: Link, startStrings: String*): Option[Link] = {
		if (startStrings.exists { s => link.destination.startsWith(s) }) None
		else Some(link)
	}
	def filterImages(link: Link): Option[Link] = filterStartsWith(link, "Image:")
	def filterFiles(link: Link): Option[Link] = filterStartsWith(link, "File:")
	def filterCategories(link: Link): Option[Link] = filterStartsWith(link, "Category:")

	/**
	 * Filters external links that are not recognized by the parser, because the markup
	 * had some errors, e.g. if the user used double brackets for external links like
	 * [[http://www.google.de]].
	 * @return None, if it is an external link, Some(link) otherwise.
	 */
	def filterExternalLinks(link: Link): Option[Link] = {
		if (link.destination.toLowerCase.startsWith("http://"))
			None
		else Some(link)
	}

}
