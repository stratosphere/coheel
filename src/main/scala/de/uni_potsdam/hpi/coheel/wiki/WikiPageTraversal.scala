package de.uni_potsdam.hpi.coheel.wiki

import de.uni_potsdam.hpi.coheel.programs.DataClasses.Link
import org.apache.flink.shaded.com.google.common.collect.{Range, TreeRangeMap}
import org.sweble.wikitext.parser.nodes._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try

/**
 * Stores information for the depth-first search of the document AST.
 * @param node The node to handle.
 * @param insideTemplateLevel The current level of nested templates.
 *                       Important because template parameters need an recursive invocation
 *                       of the parser, because they are not parsed by default.
 *                       Therefore, we
 */
case class NodeTraversalItem(node: WtNode, insideTemplateLevel: Int, ignoreText: Boolean)

/**
 * Indicator
 * We process the wiki page tree with a stack-based depth-first approach.
 * To now, when we finished traversing an template and all of it's subnodes,
 * we use the following class as a marker element.
 */
class WtTemplateClose extends WtInnerNode2(de.fau.cs.osr.ptk.common.ast.Uninitialized.X) { override def getChildNames = ??? }

class WikiPageTraversal(protected val extractor: Extractor) {

	val sb    = new StringBuilder
	val linkOffsets = TreeRangeMap.create[Integer, Link]()

	def getPlainText: String = { sb.toString() }
	def getLinkOffsets: TreeRangeMap[Integer, Link] = { linkOffsets }

	def traversePage(rootNode: WtNode): Unit = {
		nodeIterator(rootNode) { nodeItem =>
			val NodeTraversalItem(node, insideTemplate, ignoreText) = nodeItem
			node match {
				case n: WtText =>
					// we are still inside a template, if the inside template level is at least one
					visit(n, insideTemplate > 0, ignoreText)
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

	val templateBlackList = Seq("redirect", "rp", "convert", "class", "ipa", "verify", "refimprove", "see also", "refn", "citation",
		"cite", "language", "iso ", "clarification", "clarify", "reflist", "refbegin")
	// Private helper function to do depth-first search in the node tree
	private def nodeIterator(startNode: WtNode)(nodeHandlerFunction: NodeTraversalItem => Unit): Unit = {
		// The next nodes to process
		val nodeStack = mutable.Stack(NodeTraversalItem(startNode, 0, false))

		/**
		 * Aggregate all text inside templates for performance reasons.
		 * This way, only one recursive Extractor instance is need, instead of one
		 * per template parameter.
		 */
		val aggregatedTemplateSource = new StringBuilder()
		while (nodeStack.nonEmpty) {
			val nodeItem = nodeStack.pop()
			val NodeTraversalItem(node, insideTemplateLevel, ignoreText) = nodeItem
//			println(s"${"\t" * level} ${node.getClass.getSimpleName} ${Try(node.asInstanceOf[WtText].getContent.replaceAll("[\n\t]", "").trim).getOrElse("")}")
			if (node != null) {
				nodeHandlerFunction(nodeItem)
				node match {
					case t: WtTemplateClose if insideTemplateLevel == 0 =>
						// if we reached an outermost template ..
						// .. recursively call the extractor, to get the links and text inside templates.
						val sourceString = aggregatedTemplateSource.toString()
						val sourceStart = sourceString.take(templateBlackList.map(_.length).max)
						if (sourceString.nonEmpty) {
							val isBlackListed = templateBlackList.exists(sourceStart.toLowerCase.startsWith)
							val templatePage = WikiPage.fromSource(extractor.wikiPage.pageTitle, sourceString)
							val newExtractor = new Extractor(templatePage, extractor.surfaceRepr)
							nodeStack.push(NodeTraversalItem(newExtractor.rootNode, 0, isBlackListed))
						}
						aggregatedTemplateSource.clear()
					case t: WtTemplate =>
						// add a template end indicator first, then add all the template sub-nodes
						// once we reach the template close again, we know we finished that template
						nodeStack.push(NodeTraversalItem(new WtTemplateClose, insideTemplateLevel, ignoreText))
						nodeStack.pushAll(node.iterator().toSeq.reverseMap(NodeTraversalItem(_, insideTemplateLevel + 1, ignoreText)))
					case txt: WtText if insideTemplateLevel > 0 =>
						// collect text inside one template
						val source = txt.getContent.trim
						if (source.length > 2 &&
							(!ignoreText || (source.contains("[[") && source.contains("]]"))) &&
							Try(source.toInt > 1900 && source.toInt < 2100).getOrElse(true)) {
							aggregatedTemplateSource.append(s"$source ")
						}
					case n: WtTemplateArgument =>
						// drop the first template argument, because it's just the name of the parameter
						val templateChildren = n.iterator().toSeq
						nodeStack.pushAll(templateChildren.drop(1).reverseMap(NodeTraversalItem(_, insideTemplateLevel, ignoreText)))
						nodeStack.pushAll(templateChildren.take(1).reverseMap(NodeTraversalItem(_, insideTemplateLevel, true)))
					// do not go deeper into internal links, tags
					case n: WtInternalLink =>
					case n: WtExternalLink =>
					case t: WtImageLink =>
						val imageChildren = t.iterator().toSeq
						nodeStack.pushAll(imageChildren.drop(1).reverseMap(NodeTraversalItem(_, insideTemplateLevel, ignoreText)))
						nodeStack.pushAll(imageChildren.take(1).reverseMap(NodeTraversalItem(_, insideTemplateLevel, true)))
//					case n: WtPageName if inside =>
					case t: WtTagExtension =>
					case _ =>
						// on default, just push all sub nodes
						val children = node.iterator().toSeq
						nodeStack.pushAll(children.reverseMap(NodeTraversalItem(_, insideTemplateLevel, ignoreText)))
				}
			}
		}
	}

	def visit(text: WtText, insideTemplate: Boolean, ignoreText: Boolean) {
		if (!insideTemplate && !ignoreText)
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
		linkOption.foreach { link =>
			val start = sb.length + 1
			val range = Range.closedOpen(new Integer(start), new Integer(start + link.surface.length))
			linkOffsets.put(range, link)
			write(link.surface)
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

