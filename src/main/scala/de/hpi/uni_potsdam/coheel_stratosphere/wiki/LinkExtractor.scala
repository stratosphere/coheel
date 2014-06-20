package de.hpi.uni_potsdam.coheel_stratosphere.wiki

import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.InternalLinkNode
import org.dbpedia.extraction.wikiparser.Node
import scala.collection.mutable
import de.hpi.uni_potsdam.coheel_stratosphere.wiki.wikiparser.SimpleWikiParser

/**
 * Represents a link in a Wikipedia article.
 * @param sourcePage The page the link is on, e.g. 'Germany'
 * @param text The link's text, e.g. 'Merkel'
 * @param destinationPage The link's destination, e.g. 'Angela Merkel'
 */
// Note: In contrast to InternalLink, this class does not contain a Node, because
// that should not be part of the interface of this class.
case class Link(sourcePage: String, text: String, destinationPage: String)

class LinkExtractor {

	/**
	 * Internal class for processing a possible link.
	 * @param node The XML node.
	 * @param text The link's text.
	 * @param destination The link's destination.
	 */
	protected case class InternalLink(node: Node, var text: String, var destination: String) {
		def this(node: Node) = this(node, null, null)
	}

	var links: Seq[Link] = _
	var currentWikiTitle: String = _
	def extractLinks(wikiPage: WikiPage): Seq[Link] = {
		currentWikiTitle = wikiPage.title.decodedWithNamespace
		val wikiParser = new SimpleWikiParser()
		val ast = wikiParser.apply(wikiPage)
		val links = walkAST(ast)
		links

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
		val link: Option[InternalLink] = Some(new InternalLink(node))
		link
			.flatMap(filterNonLinks)
//			.flatMap(debugPrintAllLinks)
			.flatMap(filterImages)
			.flatMap(filterFiles)
			.flatMap(filterCategories)
			.flatMap(removeAnchorLinks)
			.flatMap(filterExternalLinks)
			.flatMap(toLink)
			.foreach { link =>
				links = links :+ link
			}
	}

	/**
	 * Filters out a wikiparser.Node, if it is not an internal link.
	 * @return Some(link), if it is a internal link, None otherwise.
	 */
	def filterNonLinks(link: InternalLink): Option[InternalLink] = {
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
	 * Prints all links at the current stage.
	 * This can be used for debugging.
	 * @return The unaltered link.
	 */
	def debugPrintAllLinks(link: InternalLink): Option[InternalLink] = {
		println(link.text + "#" + link.destination)
		Some(link)
	}

	/**
	 * Filters out a link, if it starts with a given string, e.g. 'Image:' or
	 * 'Category'.
	 * @param startStrings The strings to check for.
	 * @return Some(link) if the link does not start with the given string,
	 *         None otherwise.
	 */
	def filterStartsWith(link: InternalLink, startStrings: String*): Option[InternalLink] = {
		if (startStrings.exists { s => link.destination.startsWith(s) }) None
		else Some(link)
	}
	def filterImages(link: InternalLink): Option[InternalLink] = filterStartsWith(link, "Image:")
	def filterFiles(link: InternalLink): Option[InternalLink] = filterStartsWith(link, "File:")
	def filterCategories(link: InternalLink): Option[InternalLink] = filterStartsWith(link, "Category:")

	/**
	 * Handles anchor links like Germany#History (link to a specific point in
	 * a page) and removes the part after '#'
	 * @return The sanitized link.
	 */
	def removeAnchorLinks(link: InternalLink): Option[InternalLink] = {
		if (link.text == "")
			link.text = link.destination
		val hashTagIndex = link.text.indexOf("#")
		// if a hashtag was found, but not on the first position
		if (hashTagIndex != -1 && hashTagIndex != 0)
			link.text = link.text.substring(0, hashTagIndex)
		Some(link)
	}

	/**
	 * Filters external links that are not recognized by the parser, because the markup
	 * had some errors, e.g. if the user used double brackets for external links like
	 * [[http://www.google.de]].
	 * @return None, if it is an external link, Some(link) otherwise.
	 */
	def filterExternalLinks(link: InternalLink): Option[InternalLink] = {
		if (link.destination.toLowerCase.startsWith("http://"))
			None
		else Some(link)
	}

	/**
	 * Translates an internal link to an link, that can be exposed to the user.
	 */
	def toLink(link: InternalLink): Option[Link] = {
		Some(Link(currentWikiTitle, link.text, link.destination))
	}
}
