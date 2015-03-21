package de.uni_potsdam.hpi.coheel.wiki

import de.uni_potsdam.hpi.coheel.{Timer, PerformanceTimer}
import de.uni_potsdam.hpi.coheel.programs.DataClasses.Link
import org.sweble.wikitext.parser.nodes._
import scala.collection.JavaConversions._

import scala.collection.mutable

/**
 * Stores information for the depth-first search of the document AST.
 * @param node The node to handle.
 * @param insideTemplateLevel The current level of nested templates.
 *                       Important because template parameters need an recursive invocation
 *                       of the parser, because they are not parsed by default.
 *                       Therefore, we
 */
case class NodeTraversalItem(node: WtNode, insideTemplateLevel: Int)

/**
 * Indicator
 * We process the wiki page tree with a stack-based depth-first approach.
 * To now, when we finished traversing an template and all of it's subnodes,
 * we use class as a marker element.
 */
class WtTemplateClose extends WtTemplate

class WikiPageTraversal(protected val extractor: Extractor) {

	val sb    = new StringBuilder
	val links = mutable.ArrayBuffer[Link]()

	def getPlainText: String = { sb.toString() }
	def getLinks: mutable.ArrayBuffer[Link] = { links }

	def traversePage(rootNode: WtNode): Unit = {
		nodeIterator(rootNode) { nodeItem =>
			val NodeTraversalItem(node, insideTemplate) = nodeItem
			node match {
				case n: WtText =>
					// we are still inside a template, if the inside template level is at least one
					visit(n, insideTemplate > 0)
				case n: WtUrl =>
					visit(n)
				case n: WtInternalLink =>
					visit(n)
				case n: WtXmlEntityRef =>
					visit(n)
				case _ =>
			}
		}
	}

	// Private helper function to do depth-first search in the node tree
	private def nodeIterator(startNode: WtNode)(nodeHandlerFunction: NodeTraversalItem => Unit): Unit = {
		// The next nodes to process
		val nodeStack = mutable.Stack(NodeTraversalItem(startNode, 0))

		/**
		 * Aggregate all text inside templates for performance reasons.
		 * This way, only one recursive Extractor instance is need, instead of one
		 * per template parameter.
		 */
		val aggregatedTemplateSource = new StringBuilder()
		while (nodeStack.nonEmpty) {
			val nodeItem = nodeStack.pop()
			val NodeTraversalItem(node, insideTemplateLevel) = nodeItem
			if (node != null) {
				nodeHandlerFunction(nodeItem)
				node match {
					case t: WtTemplateClose =>
						// if we reached an outermost template ..
						if (insideTemplateLevel == 0) {
							// .. recursively call the extractor, to get the links and text inside templates.
							val sourceString = aggregatedTemplateSource.toString()
							if (sourceString.nonEmpty) {
								val templatePage = WikiPage.fromSource(extractor.wikiPage.pageTitle, sourceString)
								val newExtractor = new Extractor(templatePage, extractor.surfaceRepr)
								nodeStack.push(NodeTraversalItem(newExtractor.rootNode, 0))
							}
							aggregatedTemplateSource.clear()
						}
					case t: WtTemplate =>
						// add a template end indicator first, then add all the template sub-nodes
						// once we reach the template close again, we now we finished that template
						nodeStack.push(NodeTraversalItem(new WtTemplateClose, insideTemplateLevel))
						nodeStack.pushAll(node.iterator().toSeq.reverseMap(NodeTraversalItem(_, insideTemplateLevel + 1)))
//					case a: WtTemplateArgument =>
//						a.iterator()
					case txt: WtText if insideTemplateLevel > 0 =>
						// collect text inside one template
						val source = txt.getContent.trim
						aggregatedTemplateSource.append(s"$source ")
					// do not go deeper into internal links
					case t: WtInternalLink =>
					case _ =>
						// on default, just push all sub nodes
						nodeStack.pushAll(node.iterator().toSeq.reverseMap(NodeTraversalItem(_, insideTemplateLevel)))
				}
			}
		}
	}

	def visit(text: WtText, insideTemplate: Boolean) {
		if (!insideTemplate)
			write(text.getContent)
	}

	def visit(er: WtXmlEntityRef) {
		val ch = er.getResolved
		if (ch != null) {
			write(ch)
		}
	}

	def visit(url: WtUrl) {
		write(url.getProtocol)
		write(":")
		write(url.getPath)
	}

	def visit(internalLink: WtInternalLink) {
		val linkOption = extractor.extractPotentialLink(internalLink)
		linkOption match {
			case Some(link) =>
				links += link
				write(link.surface)
			case None =>
		}
	}

	private def write(s: String) {
		if (s.isEmpty)
			return
		if (sb.nonEmpty) {
			sb.append(' ')
		}
		sb.append(s)
	}
}

