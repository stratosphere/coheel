package de.hpi.uni_potsdam.coheel_stratosphere;

import org.dbpedia.extraction.sources.WikiPage;
import org.dbpedia.extraction.util.Language;
import org.dbpedia.extraction.util.WikiApi;
import org.dbpedia.extraction.wikiparser.LinkNode;
import org.dbpedia.extraction.wikiparser.Node;
import org.dbpedia.extraction.wikiparser.PageNode;
import org.dbpedia.extraction.wikiparser.WikiParser;
import org.dbpedia.extraction.wikiparser.impl.simple.SimpleWikiParser;
import scala.collection.Iterable;
import scala.collection.Iterator;
import scala.collection.JavaConverters;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class Main {

	public static void main(String[] args) throws MalformedURLException {
		WikiApi wikiApi = new WikiApi(new URL("http://en.wikipedia.org/w/api.php"), Language.English());
		List<Object> pageId = new ArrayList<>();
//		pageId.add(41528);
		pageId.add(4447008);
		Iterable<Object> it = JavaConverters.asScalaIterableConverter(pageId).asScala();
		WikiPage wikiPage = wikiApi.retrievePagesByPageID(it).toList().head();

		WikiParser wikiParser = new SimpleWikiParser();
		PageNode ast = wikiParser.apply(wikiPage);
		findLinks(ast);
	}

	private static void findLinks(Node ast) {
		Iterator<Node> it = ast.children().iterator();
		while (it.hasNext()) {
			Node node = it.next();
			if (node instanceof LinkNode) {
				LinkNode linkNode = (LinkNode) node;
//				System.out.println(node.toPlainText());
				System.out.println(linkNode.toWikiText());
			}
			Iterator<Node> nodeIt = node.children().iterator();
			while (nodeIt.hasNext()) {
				findLinks(nodeIt.next());
			}
		}


	}
}
