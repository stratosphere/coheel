package de.uni_potsdam.hpi.coheel.wiki

import java.util
import java.util.regex.Pattern
import org.sweble.wikitext.engine.Page
import org.sweble.wikitext.engine.utils.EntityReferences
import org.sweble.wikitext.engine.utils.SimpleWikiConfiguration
import org.sweble.wikitext.`lazy`.encval.IllegalCodePoint
import org.sweble.wikitext.`lazy`.parser._
import org.sweble.wikitext.`lazy`.preprocessor.TagExtension
import org.sweble.wikitext.`lazy`.preprocessor.Template
import org.sweble.wikitext.`lazy`.preprocessor.TemplateArgument
import org.sweble.wikitext.`lazy`.preprocessor.TemplateParameter
import org.sweble.wikitext.`lazy`.preprocessor.XmlComment
import org.sweble.wikitext.`lazy`.utils.XmlCharRef
import org.sweble.wikitext.`lazy`.utils.XmlEntityRef
import de.fau.cs.osr.ptk.common.Visitor
import de.fau.cs.osr.ptk.common.ast.AstNode
import de.fau.cs.osr.ptk.common.ast.NodeList
import de.fau.cs.osr.ptk.common.ast.Text
import de.fau.cs.osr.utils.StringUtils

class PlainTextConverter(private val config: SimpleWikiConfiguration, private val extractor: Extractor) extends Visitor {

	private val enumerateSections = true
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

	protected override def before(node: AstNode): Boolean = {
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

	protected override def after(node: AstNode, result: AnyRef): AnyRef = {
		finishLine()
		sb.toString()
	}

	def visit(n: AstNode) = {
		iterate(n)
	}

	def visit(n: NodeList) {
		iterate(n)
	}

	def visit(p: Page) {
		iterate(p.getContent)
	}

	def visit(t: Table) {
		iterate(t.getBody)
	}

	def visit(tr: TableRow) {
		iterate(tr.getBody)
	}

	def visit(tc: TableCell) {
		iterate(tc.getBody)
	}

	def visit(tc: TableCaption) {
		iterate(tc.getBody)
	}

	def visit(text: Text) {
		write(text.getContent)
	}

//	def visit(t: Heading) = {
//		throw new Exception("INSIDE HEADING")
//	}

	def visit(w: Whitespace) {
		write(" ")
	}

	def visit(b: Bold) {
		iterate(b.getContent)
	}

	def visit(i: Italics) {
		iterate(i.getContent)
	}

	def visit(cr: XmlCharRef) {
		write(java.lang.Character.toChars(cr.getCodePoint))
	}

	def visit(er: XmlEntityRef) {
		val ch = EntityReferences.resolve(er.getName)
		if (ch == null) {
			write('&')
			write(er.getName)
			write(';')
		} else {
			write(ch)
		}
	}

	def visit(url: Url) {
		write(url.getProtocol)
		write(':')
		write(url.getPath)
	}

	def visit(link: ExternalLink) {
		write('[')
		iterate(link.getTitle)
		write(']')
	}

	def visit(link: InternalLink) {
		val links = extractor.extractLinks(new NodeList(link))
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

	def visit(s: Section) {
		finishLine()
		val saveSb = sb
		val saveNoWrap = noWrap
		sb = new StringBuilder()
		noWrap = true
		iterate(s.getTitle)
		finishLine()
		var title = sb.toString().trim()
		sb = saveSb
		if (s.getLevel >= 1) {
			while (sections.size > s.getLevel) {
				sections.removeLast()
			}
			while (sections.size < s.getLevel) {
				sections.add(1)
			}
			if (enumerateSections) {
				val sb2 = new StringBuilder()
				for (i <- 0 until sections.size) {
					if (i < 1) {
						//continue
					}
					sb2.append(sections.get(i))
					sb2.append('.')
				}
				if (sb2.length > 0) {
					sb2.append(' ')
				}
				sb2.append(title)
				title = sb2.toString()
			}
		}
		newline(1)
		write(title)
		newline(1)
		noWrap = saveNoWrap
		iterate(s.getBody)
		while (sections.size > s.getLevel) {
			sections.removeLast()
		}
		sections.add(sections.removeLast() + 1)
	}

	def visit(p: Paragraph) {
		iterate(p.getContent)
		newline(1)
	}

	def visit(hr: HorizontalRule) {
		newline(1)
	}

	def visit(e: XmlElement) {
		if (e.getName.equalsIgnoreCase("br")) {
			newline(1)
		} else {
			iterate(e.getBody)
		}
	}

	def visit(n: Itemization) {
		iterate(n.getContent)
	}

	def visit(n: ItemizationItem) {
		iterate(n.getContent)
		newline(1)
	}

	def visit(n: ImageLink) {
	}

	def visit(n: IllegalCodePoint) {
	}

	def visit(n: XmlComment) {
	}

	def visit(n: Template) {
	}

	def visit(n: TemplateArgument) {
	}

	def visit(n: TemplateParameter) {
	}

	def visit(n: TagExtension) {
	}

	def visit(n: MagicWord) {
	}

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

