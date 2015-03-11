package de.uni_potsdam.hpi.coheel.wiki

import java.util
import java.util.regex.Pattern
import de.fau.cs.osr.ptk.common.AstVisitor
import de.fau.cs.osr.ptk.common.ast.AstNode
import de.fau.cs.osr.utils.StringUtils
import org.sweble.wikitext.engine.config.WikiConfig
import org.sweble.wikitext.parser.nodes.WtNodeList.WtNodeListImpl
import org.sweble.wikitext.parser.nodes._

class PlainTextConverter(private val config: WikiConfig, private val extractor: Extractor) extends AstVisitor[WtNode] {

	private val wrapCol = Int.MaxValue

	private val ws = Pattern.compile("\\s+")

	private var sb: StringBuilder = _

	private var line: StringBuilder = _

	private var extLinkNum: Int = _

	private var pastBod: Boolean = _

	private var needNewlines: Int = _

	private var needSpace: Boolean = _

	private var noWrap: Boolean = _

	private var sections: util.LinkedList[Integer] = _

	protected override def before(node: WtNode): Boolean = {
		sb = new StringBuilder()
		line = new StringBuilder()
		extLinkNum = 1
		pastBod = false
		needNewlines = 0
		needSpace = false
		noWrap = false
		sections = new util.LinkedList[Integer]()
		super.before(node)
	}

	protected override def after(node: WtNode, result: AnyRef): AnyRef = {
		finishLine()
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

//	def visit(t: Heading) = {
//		throw new Exception("INSIDE HEADING")
//	}

	def visit(w: WtWhitespace) {
		write(" ")
	}

	def visit(b: WtBold) {
		iterate(b)
	}

	def visit(i: WtItalics) {
		iterate(i)
	}

	def visit(cr: WtXmlCharRef) {
		write(java.lang.Character.toChars(cr.getCodePoint))
	}

	def visit(er: WtXmlEntityRef) {
		val ch = er.getResolved
		if (ch == null) {
			write('&')
			write(er.getName)
			write(';')
		} else {
			write(ch)
		}
	}

	def visit(url: WtUrl) {
		write(url.getProtocol)
		write(':')
		write(url.getPath)
	}

	def visit(link: WtExternalLink) {
		write('[')
		iterate(link.getTitle)
		write(']')
	}

	def visit(internalLink: WtInternalLink) {
		val links = extractor.extractLinks(internalLink)
		links.headOption match {
			case Some(link) =>
//				write(">>>" + link.surface + "<<<")
				write(link.surface)
			case None =>
		}
		// OLD CODE
//		try {
//			val page = PageTitle.make(config, link.getTarget)
//			if (page.getNamespace == config.getNamespace("Category")) {
//				return
//			}
//		} catch {
//			case e: LinkTargetException =>
//		}
//		write(link.getPrefix)
//		if (link.getTitle.getContent == null || link.getTitle.getContent.isEmpty) {
//			write(link.getTarget)
//		} else {
//			iterate(link.getTitle)
//		}
//		write(link.getPostfix)
	}

	def visit(s: WtSection) {
		finishLine()
		val saveSb = sb
		val saveNoWrap = noWrap
		sb = new StringBuilder()
		noWrap = true
		iterate(s.getHeading)
		finishLine()
		var title = sb.toString.trim()
		sb = saveSb
		if (s.getLevel >= 1) {
			while (sections.size > s.getLevel) { sections.removeLast() }
			while (sections.size < s.getLevel) { sections.add(1) }
			val sb2 = new StringBuilder()
			for (i <- 0 until sections.size) {
				sb2.append(sections.get(i))
				sb2.append('.')
			}
			if (sb2.length > 0) { sb2.append(' ') }
			sb2.append(title)
			title = sb2.toString
		}
		newline(2)
		write(title)
		newline(1)
		write(StringUtils.strrep('-', title.length))
		newline(2)
		noWrap = saveNoWrap
		iterate(s.getBody)
		while (sections.size > s.getLevel) {
			sections.removeLast()
		}
		sections.add(sections.removeLast() + 1)
	}

	def visit(p: WtParagraph) {
		iterate(p)
		newline(1)
	}

	def visit(hr: WtHorizontalRule) {
		newline(1)
	}

	def visit(e: WtXmlElement) {
		if (e.getName.equalsIgnoreCase("br")) {
			newline(1)
		} else {
			iterate(e.getBody)
		}
	}

	def visit(n: WtListItem) {
		iterate(n)
	}

	def visit(n: WtIllegalCodePoint) { }

	def visit(n: WtXmlComment) { }

	def visit(n: WtTemplate) { }

	def visit(n: WtTemplateArgument) { }

	def visit(n: WtTemplateParameter) { }

	def visit(n: WtTagExtension) { }

	private def newline(num: Int) {
		if (pastBod) {
			if (num > needNewlines) {
				needNewlines = num
			}
		}
	}

	private def wantSpace() {
		if (pastBod) {
			needSpace = true
		}
	}

	private def finishLine() {
		sb.append(line)
		line.setLength(0)
	}

	private def writeNewlines(num: Int) {
		finishLine()
		sb.append(StringUtils.strrep('\n', num))
		needNewlines = 0
		needSpace = false
	}

	private def writeWord(s: String) {
		var length = s.length
		if (length == 0) {
			return
		}
		if (!noWrap && needNewlines <= 0) {
			if (needSpace) {
				length += 1
			}
			if (line.length + length >= wrapCol && line.length > 0) {
				writeNewlines(1)
			}
		}
		if (needSpace && needNewlines <= 0) {
			line.append(' ')
		}
		if (needNewlines > 0) {
			writeNewlines(needNewlines)
		}
		needSpace = false
		pastBod = true
		line.append(s)
	}

	private def write(s: String) {
		if (s.isEmpty)
			return
		if (java.lang.Character.isSpaceChar(s.charAt(0)))
			wantSpace()
		val words = ws.split(s)
		var i = 0
		while (i < words.length) {
			writeWord(words(i))
			if (i < words.length) {
				wantSpace()
			}
			i += 1
		}
		if (java.lang.Character.isSpaceChar(s.charAt(s.length - 1)))
			wantSpace()
	}

	private def write(cs: Array[Char]) {
		write(String.valueOf(cs))
	}

	private def write(ch: Char) {
		writeWord(String.valueOf(ch))
	}
}

