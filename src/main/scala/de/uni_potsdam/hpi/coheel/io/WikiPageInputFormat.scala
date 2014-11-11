package de.uni_potsdam.hpi.coheel.io

import de.uni_potsdam.hpi.coheel.wiki.WikiPage
import org.apache.flink.api.common.io.{DelimitedInputFormat, FileInputFormat}

//class WikiPageInputFormat extends FileInputFormat[String] {
class WikiPageInputFormat extends DelimitedInputFormat[String] {

	setDelimiter("<page>")
	override def readRecord(reuse: String, bytes: Array[Byte], offset: Int, numBytes: Int): String = {
//		println(s"${bytes.size} $offset $numBytes")
		new String(bytes, offset, numBytes)
	}

}

class Foo extends FileInputFormat[WikiPage] {
	override def reachedEnd(): Boolean = {
		true

	}

	override def nextRecord(reuse: WikiPage): WikiPage = {
		stream
		null
	}
}
