package de.uni_potsdam.hpi.coheel

import org.apache.flink.util.Collector
import scala.collection.mutable

class TestCollector[T] extends Collector[T] {

	val collected = mutable.ArrayBuffer[T]()

	override def collect(record: T): Unit = {
		collected += record
	}
	override def close(): Unit = {}
}
