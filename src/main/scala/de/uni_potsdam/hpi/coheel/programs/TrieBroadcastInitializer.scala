package de.uni_potsdam.hpi.coheel.programs

import java.util.Date

import de.uni_potsdam.hpi.coheel.datastructures.NewTrie
import de.uni_potsdam.hpi.coheel.debugging.FreeMemory
import org.apache.flink.api.common.functions.{RichFlatMapFunction, BroadcastVariableInitializer}
import org.apache.flink.configuration.Configuration
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

class TrieWithProbBroadcastInitializer extends BroadcastVariableInitializer[(String, Float), NewTrie] {

	override def initializeBroadcastVariable(surfaces: Iterable[(String, Float)]): NewTrie = {
		val trieFromBroadcast = new NewTrie
		surfaces.asScala.foreach { case (surface, tokenProb) =>
			trieFromBroadcast.add(surface, tokenProb)
		}
		trieFromBroadcast
	}
}

object SurfacesInTrieFlatMap {
	val BROADCAST_SURFACES = "surfaces"
}

/**
  * Abstract base class for flatmaps needing access to the trie, whose values were broadcasted.
  */
abstract class SurfacesInTrieFlatMap[I, O] extends RichFlatMapFunction[I, O] {
	import CoheelLogger._
	var trie: NewTrie = _

	override def open(params: Configuration): Unit = {
		log.info(s"Building trie with ${FreeMemory.get(true)} MB")
		val d1 = new Date

		trie = getRuntimeContext.getBroadcastVariableWithInitializer(SurfacesInTrieFlatMap.BROADCAST_SURFACES, new TrieBroadcastInitializer)
		log.info(s"Finished trie with ${FreeMemory.get(true)} MB in ${(new Date().getTime - d1.getTime) / 1000} s")
	}

}
