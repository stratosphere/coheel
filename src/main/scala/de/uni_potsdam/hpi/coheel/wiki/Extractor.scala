package de.uni_potsdam.hpi.coheel.wiki

import de.uni_potsdam.hpi.coheel.programs.DataClasses.Link

import scala.collection.immutable.Queue
import scala.collection.mutable
import org.sweble.wikitext.engine._
import scala.collection.JavaConversions._
import org.sweble.wikitext.engine.utils.SimpleWikiConfiguration
import de.fau.cs.osr.ptk.common.ast.{ContentNode, Text, AstNode, NodeList}
import org.sweble.wikitext.`lazy`.parser.{Bold, Paragraph, InternalLink}


class Extractor(wikiPage: WikiPage) {

	val config = new SimpleWikiConfiguration(
		"classpath:/org/sweble/wikitext/engine/SimpleWikiConfiguration.xml")
	/**
	 * Internal class for processing a possible link.
	 * @param node The XML node.
	 * @param text The link's text.
	 * @param destination The link's destination.
	 */
	protected case class LinkWithNode(node: AstNode, var text: String, var destination: String) {
		def this(node: AstNode) = this(node, null, null)
	}

	val compiledWikiPage = getCompiledWikiPage(wikiPage)
	var links: Seq[Link] = _

	def extractAllLinks(filterEmptySurfaceRepr: Boolean = true): Seq[Link] = {
		val allLinks =  (extractLinks() ++ extractAlternativeNames())
		if (filterEmptySurfaceRepr)
			allLinks.filter { link => link.surfaceRepr.nonEmpty }
		else
			allLinks
	}

	def extractLinks(): Seq[Link] = {
		val rootNode = compiledWikiPage.getContent
		val links = extractLinks(rootNode)
		links
	}

	/**
	 * Extracts plain text, with all wiki markup removed.
	 * @return The plain text of the wikipage.
	 */
	def extractPlainText(): String = {
		val plainTextConverter = new PlainTextConverter(config, this)
		val page = compiledWikiPage
		plainTextConverter.go(page).asInstanceOf[String]
	}

	/**
	 * This searches for the first paragraph in the text, and returns all bold texts within that first paragraph.
	 * These are supposed to be alternative names for the entity.
	 * @return A list of alternative names
	 */
	def extractAlternativeNames(): Queue[Link] = {
		// The minimum number of characters for the first paragraph
		val MIN_PARAGRAPH_LENGTH = 20
		val rootNode = compiledWikiPage.getContent

		nodeIterator(rootNode) {
			case paragraph: Paragraph =>
				val paragraphText = getText(paragraph)
				if (paragraphText.length > MIN_PARAGRAPH_LENGTH) {
					return extractBoldWordsFrom(paragraph)
				}
			case _ =>
		}
		Queue()
	}

	private def extractBoldWordsFrom(paragraph: Paragraph): Queue[Link] = {
		var boldWords = Queue[String]()
		nodeIterator(paragraph) {
			case bold: Bold =>
				val text = getText(bold).trim
				if (text.nonEmpty) // TODO: Check why texts can be empty
					boldWords = boldWords.enqueue(text)
			case _ =>
		}
		boldWords.map { word => Link(word, TokenizerHelper.transformToTokenized(word), wikiPage.pageTitle, wikiPage.pageTitle) }
	}

	// Private helper function to extract breadth-first search in the node tree
	private def nodeIterator(startNode: AstNode)(nodeHandlerFunction: AstNode => Unit): Unit = {
		val nodeQueue = mutable.Queue[AstNode](startNode)
		while (nodeQueue.nonEmpty) {
			val node = nodeQueue.dequeue()
			if (node != null) {
				nodeHandlerFunction(node)
				nodeQueue.enqueue(node.iterator().toList: _*)
			}
		}
	}

	private def getCompiledWikiPage(wikiPage: WikiPage): Page = {
		val compiler = new Compiler(config)
		val pageTitle = PageTitle.make(config, wikiPage.pageTitle)
		val pageId = new PageId(pageTitle, 0)

		val page = compiler.postprocess(pageId, wikiPage.source, null).getPage

		page
	}

	protected[wiki] def extractLinks(parentNode: NodeList): Seq[Link] = {
		links = Vector()
		nodeIterator(parentNode) { node =>
			extractPotentialLink(node)
		}
		links
	}

	private def extractPotentialLink(node: AstNode): Unit = {
		val link: Option[LinkWithNode] = Some(new LinkWithNode(node))
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

	private def getText(link: ContentNode): String = {
		link.getContent.flatMap {
			case textNode: Text =>
				Some(textNode.getContent)
			case otherNode: ContentNode =>
				Some(getText(otherNode))
			case _ => None
		}.mkString("")
	}

	/**
	 * Filters out a wikiparser.Node, if it is not an internal link.
	 * @return Some(link), if it is a internal link, None otherwise.
	 */
	private def filterNonLinks(link: LinkWithNode): Option[LinkWithNode] = {
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
	private def debugPrintAllLinks(link: LinkWithNode): Option[LinkWithNode] = {
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
	private def filterStartsWith(link: LinkWithNode, startStrings: String*): Option[LinkWithNode] = {
		if (startStrings.exists { s =>
			link.destination.startsWith(s) ||
			link.destination.startsWith(s":$s") }) None
		else Some(link)
	}
	private def filterImages(link: LinkWithNode): Option[LinkWithNode] = filterStartsWith(link, "Image:")
	private def filterFiles(link: LinkWithNode): Option[LinkWithNode] = filterStartsWith(link, "File:")
	private def filterCategories(link: LinkWithNode): Option[LinkWithNode] = filterStartsWith(link, "Category:")

	/**
	 * Handles anchor links like Germany#History (link to a specific point in
	 * a page) and removes the part after '#'
	 * @return The sanitized link.
	 */
	private def removeAnchorLinks(link: LinkWithNode): Option[LinkWithNode] = {
		if (link.text.trim == "")
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
	private def trimWhitespace(link: LinkWithNode): Option[LinkWithNode] = {
		link.text = link.text.trim
		while (link.text.startsWith("\"") && link.text.endsWith("\"") && link.text.size > 1)
			link.text = link.text.drop(1).dropRight(1)
		Some(link)
	}

	/**
	 * Filters external links that are not recognized by the parser, because the markup
	 * had some errors, e.g. if the user used double brackets for external links like
	 * [[http://www.google.de]].
	 * @return None, if it is an external link, Some(link) otherwise.
	 */
	private def filterExternalLinks(link: LinkWithNode): Option[LinkWithNode] = {
		if (link.destination.toLowerCase.startsWith("http://"))
			None
		else Some(link)
	}

	/**
	 * Translates an internal link to an link, that can be exposed to the user.
	 */
	private def toLink(link: LinkWithNode): Option[Link] = {
		Some(Link(link.text, TokenizerHelper.transformToTokenized(link.text), wikiPage.pageTitle, link.destination))
	}
}
