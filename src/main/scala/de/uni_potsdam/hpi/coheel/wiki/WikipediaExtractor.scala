package de.uni_potsdam.hpi.coheel.wiki

import de.fau.cs.osr.ptk.common.AstVisitor
import de.uni_potsdam.hpi.coheel.programs.DataClasses.Link
import org.sweble.wikitext.parser.nodes._

import scala.collection.mutable

/**
 * PlainTextConverter.
 * Copied from Sweble's example code, and adapted to our needs.
 */
class WikipediaExtractor(protected val extractor: Extractor) extends AstVisitor[WtNode] {

	val sb                       = new StringBuilder
	val links                    = mutable.ArrayBuffer[Link]()
	var insideTemplateCount      = 0
	val aggregatedTemplateSource = new StringBuilder()

	override def go(node: WtNode): AnyRef = {
		// run visitor as normal
		super.go(node)
//		// then parse templates
		val sourceString = aggregatedTemplateSource.toString()
		if (sourceString.nonEmpty) {
//			println(sourceString)
			val templatePage = WikiPage.fromSource(extractor.wikiPage.pageTitle, sourceString)
			val subExtractor = new Extractor(templatePage, extractor.surfaceRepr)
			subExtractor.extract()
			links ++= subExtractor.getLinks()
		}
		null
	}

	def getPlainText: String = {
		sb.toString()
	}

	def getLinks: mutable.ArrayBuffer[Link] = {
		links
	}

	def visit(n: WtNode) = {
		iterate(n)
	}

	def visit(i: WtImageLink): Unit = {
		iterate(i)
	}

	def visit(n: WtNodeList) {
		iterate(n)
	}

	def visit(t: WtTable) {
		iterate(t.getBody)
	}

	def visit(tr: WtTableRow) {
		iterate(tr.getBody)
	}

	def visit(tc: WtTableCell) {
		iterate(tc.getBody)
	}

	def visit(tc: WtTableCaption) {
		iterate(tc.getBody)
	}

	def visit(text: WtText) {
		if (insideTemplateCount > 0) {
			val source = text.getContent.trim
			aggregatedTemplateSource.append(s"$source ")
			// To links between two overlapping template parameters, we introduce
			// a splitter, which must not be inside a link.
		}
		else
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

	def visit(link: WtExternalLink) {
		iterate(link.getTitle)
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

	def visit(s: WtSection) {
		iterate(s.getHeading)
		iterate(s.getBody)
	}

	def visit(e: WtXmlElement) {
		iterate(e.getBody)
	}

	def visit(n: WtIllegalCodePoint) {}
	def visit(n: WtXmlComment) {}

	def visit(n: WtXmlAttributes): Unit = {
		throw new RuntimeException("FOOBAR")

	}
	def visit(n: WtTemplate): Unit = {
		insideTemplateCount += 1
		iterate(n)
		aggregatedTemplateSource.append(s" ${extractor.TEMPLATE_SEPARATOR}\n")
		insideTemplateCount -= 1
	}
//	def visit(n: WtTemplateArgument): Unit = { }
//	def visit(n: WtTemplateParameter) {}
	def visit(n: WtTagExtension) {}

	private def write(s: String) {
		if (s.isEmpty)
			return
		if (sb.nonEmpty) {
			sb.append(' ')
		}
		sb.append(s)
	}
}

