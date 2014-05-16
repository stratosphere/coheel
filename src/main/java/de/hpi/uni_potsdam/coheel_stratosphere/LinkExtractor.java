package de.hpi.uni_potsdam.coheel_stratosphere;

import org.dbpedia.extraction.sources.WikiPage;
import org.dbpedia.extraction.wikiparser.InternalLinkNode;
import org.dbpedia.extraction.wikiparser.Node;
import org.dbpedia.extraction.wikiparser.PageNode;
import org.dbpedia.extraction.wikiparser.WikiParser;
import org.dbpedia.extraction.wikiparser.impl.simple.SimpleWikiParser;
import scala.collection.Iterator;

import java.util.LinkedList;
import java.util.Queue;

public class LinkExtractor {

	public void extractLinks(WikiPage wikiPage) {
		WikiParser wikiParser = new SimpleWikiParser();
		PageNode ast = wikiParser.apply(wikiPage);
		walkAST(ast);
	}


	private void walkAST(Node parentNode) {
		Queue<Node> nodeQueue = new LinkedList<Node>();
		nodeQueue.offer(parentNode);
		while (!nodeQueue.isEmpty()) {
			Node node =  nodeQueue.poll();
			handleNode(node);

			Iterator<Node> it = node.children().iterator();
			while (it.hasNext())
				nodeQueue.offer(it.next());

		}
	}

	private void handleNode(Node node) {
		if (!(node instanceof InternalLinkNode))
			return;
		InternalLinkNode linkNode = (InternalLinkNode) node;
		String linkDestination = linkNode.destination().decodedWithNamespace();

		if (linkDestination.startsWith("Image:"))
			return;
		if (linkDestination.startsWith("File:"))
			return;
		if (linkDestination.startsWith("Category:"))
			return;

		// there are two ways of getting the link text, check if the differ
		// somehow
		String linkText = linkNode.toPlainText();
		String otherLinkText = null;
		if (linkNode.children().nonEmpty())
			otherLinkText = linkNode.children().head().toPlainText();

		if (otherLinkText != null && !otherLinkText.equals(linkText)) {
			throw new RuntimeException(String.format("Links were not equal: >%s<, >%s<", linkText, otherLinkText));
		}

		if (linkText.equals(""))
			linkText = linkDestination;
		int hashTagIndex = linkText.indexOf("#");
		if (hashTagIndex != -1)
			linkText = linkText.substring(0, hashTagIndex);
		System.out.println(String.format("%80s||%s", linkText, linkDestination));
	}
}
