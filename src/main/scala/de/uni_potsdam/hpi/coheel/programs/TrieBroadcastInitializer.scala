package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.datastructures.NewTrie
import org.apache.flink.api.common.functions.BroadcastVariableInitializer
import scala.collection.JavaConverters._
import java.lang.Iterable

class TrieBroadcastInitializer extends BroadcastVariableInitializer[String, NewTrie] {

	override def initializeBroadcastVariable(surfaces: Iterable[String]): NewTrie = {
		val trieFromBroadcast = new NewTrie
		surfaces.asScala.foreach { surface =>
			trieFromBroadcast.add(surface)
		}
		trieFromBroadcast
	}
}

