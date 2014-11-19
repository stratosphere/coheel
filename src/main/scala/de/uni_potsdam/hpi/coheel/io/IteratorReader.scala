package de.uni_potsdam.hpi.coheel.io

import java.io.Reader
import java.io.StringReader

class IteratorReader(var iterator: scala.Iterator[String]) extends Reader {

	private var current: StringReader = _

	if (iterator == null) {
		throw new NullPointerException()
	}

	override def read(cbuf: Array[Char], off: Int, len: Int): Int = {
		if (current != null) {
			val ret = current.read(cbuf, off, len)
			if (ret > 0 || len == 0) {
				return ret
			}
		}
		if (iterator.hasNext) {
			current = new StringReader(iterator.next() + '\n')
			current.read(cbuf, off, len)
		} else {
			-1
		}
	}

	override def close() {
		iterator = null
		current = null
	}
}
