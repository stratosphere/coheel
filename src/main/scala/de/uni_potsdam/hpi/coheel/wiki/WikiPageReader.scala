package de.uni_potsdam.hpi.coheel.wiki

import java.io.{Reader, StringReader, BufferedReader}
import javax.xml.stream.{XMLStreamConstants, XMLInputFactory}
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.log4j.Logger
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import scala.collection.mutable

/**
 * Captures the important aspects of a WikiPage for our use case, while still
 * maintaning connection (via inheritance) to the DBpedia extraction framework.
 * <br />
 * <strong>Definition redirect</strong>:<br />
 * A page is seen as a redirect, if it contains the &lt;redirect&gt; tag inside the &lt;page&gt; tag in the wiki
 * dump.
 * <br />
 * <strong>Definition disambiguation</strong>:<br />
 * A page is seen as a disambiguation if one of the following conditions is true:
 * <ul>
 *     <li> The title contains "(disambiguation)"
 *     <li> The text contains a disambiguation template (not all disambiguation pages contain "(disambiguation)" in the
 *          title, e.g. http://en.wikipedia.org/wiki/Alien
 * <strong>Definition list</strong>:<br />
 * A page is seen as a list if it belongs to a category which starts with List.
 * @param pageTitle The title of the page as a string.
 * @param ns The namespace of the wiki page.
 * @param redirect The title of the page this page is redirecting to or null, if it is not a redirect.
 * @param plainText This page's plain text content as array of tokens.
 */
case class WikiPage(pageTitle: String,
                    ns: Int, redirect: String,
                    plainText: Array[String],
                    links: Array[Link],
	                isDisambiguation: Boolean,
	                isList: Boolean) {

	val isRedirect: Boolean = this.redirect != ""
	var source: String = _

	def isNormalPage: Boolean = {
		!isDisambiguation && !isRedirect && !isList
	}
}
case class FullInfoWikiPage(pageTitle: String,
                    ns: Int, redirect: String,
                    plainText: mutable.ArrayBuffer[String],
                    tags: mutable.ArrayBuffer[String],
                    links: Array[Link],
                    isDisambiguation: Boolean,
                    isList: Boolean)

object WikiPage {

	/**
	 * Builds a wiki page from the given title and wiki markup source.
	 */
	def fromSource(pageTitle:String, source: String): WikiPage = {
		val wp = WikiPage(pageTitle, 0, "", Array(), Array(), false, false)
		wp.source = source
		wp
	}
}

class WikiPageReader {

	val log = Logger.getLogger(getClass)

	val factory = XMLInputFactory.newInstance()
	def xmlToWikiPages(s: String): Iterator[WikiPage] = {
		val reader = new BufferedReader(new StringReader(s))
		xmlToWikiPages(reader)
	}

	private var readCounter = 1

	def xmlToWikiPages(reader: Reader): Iterator[WikiPage] = {
		new Iterator[WikiPage] {
			var alreadyRead = false
			var hasMorePages = true

			val streamReader = factory.createXMLStreamReader(reader)

			// Values for the current page
			var pageTitle: String = _
			var ns: Int = _
			var redirectTitle: String = _
			var text: String = _

			readNextPage()

			def readNextPage(): Unit = {
				redirectTitle = ""
				var foundNextPage = false

				while (!foundNextPage && streamReader.hasNext) {
					streamReader.next
					if (streamReader.getEventType == XMLStreamConstants.START_ELEMENT) {
						streamReader.getLocalName match {
							case "text" => text = streamReader.getElementText
							case "ns" => ns = streamReader.getElementText.toInt
							case "title" => pageTitle = StringEscapeUtils.unescapeXml(streamReader.getElementText)
							case "redirect" => redirectTitle = StringEscapeUtils.unescapeXml(streamReader.getAttributeValue(null, "title"))
							case "page" => foundNextPage = true
							case _ =>
						}
					}
				}
				hasMorePages = streamReader.hasNext
				if (!hasMorePages)
					reader.close()
			}

			def hasNext = hasMorePages
			def next(): WikiPage = {
				if (!alreadyRead) {
					alreadyRead = true
//					log.info(f"Reading $readCounter%4s. wiki file on this node.")
					readCounter += 1
				}
				readNextPage()
				val isDisambiguation = checkDisambiguation(text, pageTitle)
				val isList           = checkList(pageTitle)
				val wikiPage = new WikiPage(
					pageTitle,
					ns,
					redirectTitle,
					Array(),
					Array(),
					isDisambiguation,
					isList
				)
				wikiPage.source = text
				wikiPage
			}
		}
	}

	def checkDisambiguation(source: String, pageTitle: String): Boolean = {
		val isDisambiguationFromTitle = pageTitle.contains("(disambiguation)")
		if (isDisambiguationFromTitle)
			true
		else {
			// in case of performance problems:
			// disambiguation links are always (as seen so far) at the end of the text
			// maybe this could be used to not scan the entire text
			val disambiguationRegex = """(?ui)\{\{disambiguation.*?\}\}""".r
			val matches = disambiguationRegex.findAllIn(source)
				// check whether the regex sometimes accidentially matches too much text
				.map { s =>
				if (s.length > 200)
					log.warn(s"Disambiguation regex returns long result: $s.")
				s
			}
				.map(_.toLowerCase)
				.filter { s =>
				!s.contains("disambiguation needed")
			}
			matches.nonEmpty
		}
	}

	def checkList(pageTitle: String): Boolean = pageTitle.startsWith("List of") ||
		pageTitle.startsWith("Lists of")
}
