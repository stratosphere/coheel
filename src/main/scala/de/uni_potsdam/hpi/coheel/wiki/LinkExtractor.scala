package de.uni_potsdam.hpi.coheel.wiki

import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.InternalLinkNode
import org.dbpedia.extraction.wikiparser.Node
import scala.collection.mutable
import org.sweble.wikitext.engine.{PageId, PageTitle, Compiler}
import de.uni_potsdam.hpi.coheel.wiki.wikiparser.ExtendedSimpleWikiParser
import scala.collection.JavaConversions._
import org.sweble.wikitext.engine.utils.SimpleWikiConfiguration
import de.fau.cs.osr.ptk.common.ast.{Text, AstNode, NodeList}
import org.sweble.wikitext.`lazy`.parser.{LinkTitle, InternalLink}

/**
 * Represents a link in a Wikipedia article.
 * @param source The page the link is on, e.g. 'Germany'
 * @param text The link's text, e.g. 'Merkel'
 * @param destination The link's destination, e.g. 'Angela Merkel'
 */
// Note: In contrast to InternalLink, this class does not contain a Node, because
// that should not be part of the interface of this class.
case class Link(source: String, text: String, destination: String)

class LinkExtractor {

	/**
	 * Internal class for processing a possible link.
	 * @param node The XML node.
	 * @param text The link's text.
	 * @param destination The link's destination.
	 */
	protected case class InternalFooLink(node: AstNode, var text: String, var destination: String) {
		def this(node: AstNode) = this(node, null, null)
	}

	var links: Seq[Link] = _
	var currentWikiTitle: String = _
	def extractLinks(wikiPage: WikiPage): Seq[Link] = {
		currentWikiTitle = wikiPage.title.decodedWithNamespace
		val config = new SimpleWikiConfiguration(
			"classpath:/org/sweble/wikitext/engine/SimpleWikiConfiguration.xml")
		val compiler = new Compiler(config)
		val pageTitle = PageTitle.make(config, wikiPage.title.decodedWithNamespace)
		val pageId = new PageId(pageTitle, wikiPage.id)

		val page = compiler.postprocess(pageId, wikiPage.source, null).getPage

		val nodeList = page.getContent
		val links = walkAST(nodeList)
		links
	}

	private def walkAST(parentNode: NodeList): Seq[Link] = {
		links = Vector()
		val nodeQueue = mutable.Queue[AstNode](parentNode)
		while (!nodeQueue.isEmpty) {
			val node = nodeQueue.dequeue()
			if (node != null) {
				handleNode(node)
				nodeQueue.enqueue(node.iterator().toList: _*)
			}
		}
		links
	}

	private def handleNode(node: AstNode): Unit = {
		val link: Option[InternalFooLink] = Some(new InternalFooLink(node))
		link
			.flatMap(filterNonLinks)
//			.flatMap(debugPrintAllLinks)
			.flatMap(filterImages)
			.flatMap(filterFiles)
			.flatMap(filterCategories)
			.flatMap(removeAnchorLinks)
			.flatMap(trimWhitespace)
			.flatMap(filterExternalLinks)
			.flatMap(toLink)
			.foreach { link =>
				links = links :+ link
			}
	}

	def getText(link: LinkTitle): String = {
		link.getContent.flatMap {
			case textNode: Text =>
				Some(textNode.getContent)
			case _ => None
		}.mkString(" ")
	}

	/**
	 * Filters out a wikiparser.Node, if it is not an internal link.
	 * @return Some(link), if it is a internal link, None otherwise.
	 */
	def filterNonLinks(link: InternalFooLink): Option[InternalFooLink] = {
		if (!link.node.isInstanceOf[InternalLink])
			None
		else {
			val linkNode = link.node.asInstanceOf[InternalLink]
			link.destination = linkNode.getTarget
			link.text = getText(linkNode.getTitle)
			Some(link)
		}
	}

	/**
	 * Prints all links at the current stage.
	 * This can be used for debugging.
	 * @return The unaltered link.
	 */
	def debugPrintAllLinks(link: InternalFooLink): Option[InternalFooLink] = {
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
	def filterStartsWith(link: InternalFooLink, startStrings: String*): Option[InternalFooLink] = {
		if (startStrings.exists { s => link.destination.startsWith(s) ||
			link.destination.startsWith(s":$s") }) None
		else Some(link)
	}
	def filterImages(link: InternalFooLink): Option[InternalFooLink] = filterStartsWith(link, "Image:")
	def filterFiles(link: InternalFooLink): Option[InternalFooLink] = filterStartsWith(link, "File:")
	def filterCategories(link: InternalFooLink): Option[InternalFooLink] = filterStartsWith(link, "Category:")

	/**
	 * Handles anchor links like Germany#History (link to a specific point in
	 * a page) and removes the part after '#'
	 * @return The sanitized link.
	 */
	def removeAnchorLinks(link: InternalFooLink): Option[InternalFooLink] = {
		if (link.text == "")
			link.text = link.destination
		val hashTagIndex = link.destination.indexOf("#")
		// if a hashtag was found, but not on the first position
		if (hashTagIndex != -1 && hashTagIndex != 0)
			link.destination = link.destination.substring(0, hashTagIndex)
		Some(link)
	}

	/**
	 * Handles link texts like '  Germany ' and removes the whitespace.
	 * @return The sanitized link.
	 */
	def trimWhitespace(link: InternalFooLink): Option[InternalFooLink] = {
		link.text = link.text.trim
		Some(link)
	}

	/**
	 * Filters external links that are not recognized by the parser, because the markup
	 * had some errors, e.g. if the user used double brackets for external links like
	 * [[http://www.google.de]].
	 * @return None, if it is an external link, Some(link) otherwise.
	 */
	def filterExternalLinks(link: InternalFooLink): Option[InternalFooLink] = {
		if (link.destination.toLowerCase.startsWith("http://"))
			None
		else Some(link)
	}

	/**
	 * Translates an internal link to an link, that can be exposed to the user.
	 */
	def toLink(link: InternalFooLink): Option[Link] = {
		Some(Link(currentWikiTitle, link.text, link.destination))
	}
}
