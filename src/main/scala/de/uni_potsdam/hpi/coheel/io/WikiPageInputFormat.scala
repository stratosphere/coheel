package de.uni_potsdam.hpi.coheel.io

import org.apache.flink.api.common.io.DelimitedInputFormat

//class WikiPageInputFormat extends FileInputFormat[String] {
class WikiPageInputFormat extends DelimitedInputFormat[String] {

	setDelimiter("<MARKER />")
	override def readRecord(reuse: String, bytes: Array[Byte], offset: Int, numBytes: Int): String = {
		new String(bytes, offset, numBytes)
	}

}
