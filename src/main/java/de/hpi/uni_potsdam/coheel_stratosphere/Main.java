package de.hpi.uni_potsdam.coheel_stratosphere;

import org.dbpedia.extraction.sources.WikiPage;
import org.dbpedia.extraction.util.Language;
import org.dbpedia.extraction.util.WikiApi;
import org.dbpedia.extraction.wikiparser.*;
import org.dbpedia.extraction.wikiparser.impl.simple.SimpleWikiParser;
import scala.collection.Iterable;
import scala.collection.Iterator;
import scala.collection.JavaConverters;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class Main {

	public static void main(String[] args) {
//		WikiPage page = getExampleWikiPage(4447008); // Scintilla
//		WikiPage page = getExampleWikiPage(41528); // Forrest Gump
		WikiPage page = getExampleWikiPage(11867); // Germany
		LinkExtractor linkExtractor = new LinkExtractor();
		linkExtractor.extractLinks(page);
	}

	public static WikiPage getExampleWikiPage(int pageId) {
		WikiApi wikiApi = null;
		try {
			wikiApi = new WikiApi(
				new URL("http://en.wikipedia.org/w/api.php"),
				Language.English());
			List<Object> pageIds = new ArrayList<>();
			pageIds.add(pageId);
			Iterable<Object> it = JavaConverters.asScalaIterableConverter(pageIds).asScala();
			WikiPage wikiPage = wikiApi.retrievePagesByPageID(it).toList().head();
			return wikiPage;
		} catch (MalformedURLException e) {
			throw new RuntimeException(e);
		}
	}

}
