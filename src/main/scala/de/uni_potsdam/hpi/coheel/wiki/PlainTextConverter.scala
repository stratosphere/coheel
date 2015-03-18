package de.uni_potsdam.hpi.coheel.wiki

import java.util
import java.util.regex.Pattern
import de.fau.cs.osr.ptk.common.AstVisitor
import de.fau.cs.osr.ptk.common.ast.AstNode
import de.fau.cs.osr.utils.StringUtils
import org.sweble.wikitext.engine.config.WikiConfig
import org.sweble.wikitext.parser.nodes.WtNodeList.WtNodeListImpl
import org.sweble.wikitext.parser.nodes._

/**
 * PlainTextConverter.
 * Copied from Sweble's example code, and adapted to our needs.
 */
class PlainTextConverter(private val extractor: Extractor) extends AstVisitor[WtNode] {

	private val sb: StringBuilder = new StringBuilder

	def getPlainText: String = {
		sb.toString()
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
		write(text.getContent)
	}

	def visit(w: WtWhitespace) {
		write(" ")
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
		val links = extractor.extractLinks(internalLink)
		links.headOption match {
			case Some(link) =>
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
	def visit(n: WtTemplate) {}
	def visit(n: WtTemplateArgument) {}
	def visit(n: WtTemplateParameter) {}
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

