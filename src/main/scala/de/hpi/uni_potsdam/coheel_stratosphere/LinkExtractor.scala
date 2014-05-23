package de.hpi.uni_potsdam.coheel_stratosphere

import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.InternalLinkNode
import org.dbpedia.extraction.wikiparser.Node
import org.dbpedia.extraction.wikiparser.impl.simple.SimpleWikiParser
import java.util.LinkedList
import scala.collection.mutable

class LinkExtractor {

	def extractLinks(wikiPage: WikiPage) {
		val wikiParser = new SimpleWikiParser()
		val ast = wikiParser.apply(wikiPage)
		walkAST(ast)
	}

	private def walkAST(parentNode: Node) {
		val nodeQueue = mutable.Queue(parentNode)
		while (!nodeQueue.isEmpty) {
			val node = nodeQueue.dequeue()
			handleNode(node)
			// enqueue takes variable parameter list,
			// node.children is a list, so unfold the list
			nodeQueue.enqueue(node.children: _*)
		}
	}

	private def handleNode(node: Node) {
		if (!node.isInstanceOf[InternalLinkNode])
			return
		val linkNode = node.asInstanceOf[InternalLinkNode]
		val linkDestination = linkNode.destination.decodedWithNamespace
		if (linkDestination.startsWith("Image:"))
			return
		if (linkDestination.startsWith("File:"))
			return
		if (linkDestination.startsWith("Category:"))
			return
		if (linkNode.children.size > 2)
			throw new RuntimeException("More than one child for a link.")

		var linkText = linkNode.toPlainText
		if (linkText == "")
			linkText = linkDestination
		val hashTagIndex = linkText.indexOf("#")
		if (hashTagIndex != -1) linkText = linkText.substring(0, hashTagIndex)
		println(String.format("%80s||%s", linkText, linkDestination))
	}
}
