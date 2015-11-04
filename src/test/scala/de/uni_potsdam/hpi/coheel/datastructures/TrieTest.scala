package de.uni_potsdam.hpi.coheel.datastructures

import java.io.File

import de.uni_potsdam.hpi.coheel.io.Sample
import de.uni_potsdam.hpi.coheel.programs.CoheelProgram
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._
import scala.io.Source

@RunWith(classOf[JUnitRunner])
class TrieTest extends FunSuite {

//	buildTests(new HashTrie())
//	buildTests(new TrieToni())
	buildTests(new NewTrie())

	def buildTests[T <: Trie](trie: => Trie): Unit = {
		def newTrie(): Trie = {
			trie
		}
		val name = trie.getClass.getSimpleName

		def assertProb(condition: Boolean): Unit = {
			if (name == "NewTrie")
				assert(condition)
		}

		test(s"print a trie for $name") {
			val trie = newTrie()
			trie.add("angela", 0.1f)
			trie.add("angela merkel chancellor", 0.2f)
			trie.add("angela merkel german", 0.3f)
			trie.add("another test", 0.4f)
			trie.add("angela chancellor", 0.5f)
			assertProb(trie.contains("angela").prob == 0.1f)
			assertProb(trie.contains("angela merkel chancellor").prob == 0.2f)
			assertProb(trie.contains("angela merkel german").prob == 0.3f)
			assertProb(java.lang.Float.isNaN(trie.contains("angela merkel").prob))
			assertProb(trie.contains("another test").prob == 0.4f)
			assertProb(trie.contains("angela chancellor").prob == 0.5f)
		}

		test(s"single word queries work for $name") {
			val trie = newTrie()
			trie.add("angela", 0.5f)
			assert(trie.contains("angela").asEntry)
			assertProb(trie.contains("angela").prob == 0.5f)
		}

		test(s"later add's in middle work for $name") {
			val trie = newTrie()
			trie.add("angela merkel", 0.1f)
			trie.add("angela", 0.2f)
			assert(trie.contains("angela").asEntry)
			assert(trie.contains("angela merkel").asEntry)
			assertProb(trie.contains("angela").prob == 0.2f)
			assertProb(trie.contains("angela merkel").prob == 0.1f)
		}

		test(s"add reuses existing nodes for $name") {
			val trie = newTrie()
			trie.add("angela merkel")
			trie.add("angela dorothea")
			val testSentence = "angela dorothea"
			val result = trie.findAllIn(testSentence).toList
			assert(result.size === 1)
		}

		test(s"contains handles spaces #1 for $name") {
			val trie = newTrie()
			trie.add("angela")
			val testSentence = "angela            merkel"
			val result = trie.findAllIn(testSentence).toList
			assert(result.size === 1)
		}

		test(s"contains handles spaces #2 for $name") {
			val trie = newTrie()
			trie.add("angela merkel")
			val testSentence = "angela            merkel"
			val result = trie.findAllIn(testSentence).toList
			assert (result.size === 1)
		}

		test(s"multiple word queries #1 work for $name") {
			val trie = newTrie()
			trie.add("angela merkel", 0.5f)
			assert(trie.contains("angela merkel").asEntry)
			assertProb(trie.contains("angela merkel").prob == 0.5f)
		}

		test(s"multiple word queries #2 work for $name") {
			val trie = newTrie()
			trie.add("angela merkel chancellor", 0.5f)
			trie.add("angela merkel", 0.4f)
			val testSentence = "angela merkel"
			val result = trie.findAllIn(testSentence).toList
			assert(result.size === 1)
			assertProb(trie.contains("angela merkel").prob == 0.4f)
			assertProb(trie.contains("angela merkel chancellor").prob == 0.5f)
		}

		test(s"multiple occurrences work for $name") {
			val trie = newTrie()
			trie.add("angela")
			val testSentence = "angela angela"
			val result = trie.findAllIn(testSentence).toList
			assert(result.size === 1)
		}

		test(s"distinction between contains-asEntry and contains-asIntermediateNode for $name") {
			val trie = newTrie()
			trie.add("angela dorothea merkel", 0.5f)

			assert(trie.contains("angela").asIntermediateNode)
			assertProb(java.lang.Float.isNaN(trie.contains("angela").prob))
			assert(trie.contains("angela dorothea").asIntermediateNode)
			assertProb(java.lang.Float.isNaN(trie.contains("angela dorothea").prob))
			assert(trie.contains("angela dorothea merkel").asIntermediateNode)
			assertProb(trie.contains("angela dorothea merkel").prob == 0.5f)

			assert(!trie.contains("angela").asEntry)
			assert(!trie.contains("angela dorothea").asEntry)
			assert(trie.contains("angela dorothea merkel").asEntry)
		}

		test(s"only actually added words are considered for $name") {
			val trie = newTrie()
			trie.add("angela merkel")
			assert(!trie.contains("angela").asEntry)
		}

		test(s"multiple adds do not cause harm for $name") {
			val trie = newTrie()
			trie.add("angela merkel", 0.5f)
			assertProb(trie.contains("angela merkel").prob == 0.5f)
			trie.add("angela merkel", 0.4f)
			assertProb(trie.contains("angela merkel").prob == 0.4f)
			trie.add("angela merkel", 0.3f)
			assertProb(trie.contains("angela merkel").prob == 0.3f)
			assert(!trie.contains("angela").asEntry)
			assert(trie.contains("angela merkel").asEntry)
		}

		test(s"findAllIn finds all occurrences for $name") {
			val trie = newTrie()
			trie.add("angela merkel", 0.5f)
			trie.add("angela merkel is german", 0.4f)
			trie.add("angela", 0.3f)
			trie.add("merkel", 0.2f)

			val testSentence = "angela merkel is german"
			val result = trie.findAllIn(testSentence).toList
			val expected1 = Seq("angela", "angela merkel", "angela merkel is german", "merkel")
			expected1.foreach { expected =>
				assert(result.contains(expected))
			}
		}

		test(s"findAllIn finds atomic strings for $name") {
			val trie = newTrie()

			trie.add("angela merkel")
			trie.add("angela", 0.5f)
			trie.add("angela dorothea merkel")

			val testSentence = "angela"
			val result = trie.findAllIn(testSentence).toList
			assert(result.size === 1)
			assertProb(trie.contains("angela").prob == 0.5f)
		}

		test(s"findAllIn finds all occurrences of substrings for $name") {
			val trie = newTrie()
			trie.add("angela merkel")
			trie.add("chancellor schroeder")
			trie.add("chancellor angela merkel")

			val testSentence = "chancellor angela merkel"
			val result = trie.findAllIn(testSentence).toList
			assert(result.size === 2)
		}

		test(s"findAllIn respects word boundaries for $name") {
			val trie = newTrie()
			trie.add("ang")
			trie.add("angel")
			trie.add("i")
			trie.add("germ")
			trie.add(" merkel")

			val testSentence = "angela merkel is german"
			val result = trie.findAllIn(testSentence).toList
			assert(result.isEmpty)
		}

		//	test("sliding contains works as expected for arbitrary type") {
		//		case class Foo(a: Int, b: Double)
		//
		//		val trie = newTrie()
		//		trie.add("1 2")
		//		trie.add("1 2 3")
		//
		//		val testSentence = Array(Foo(1, 1.0), Foo(2, 2.0))
		//		val result = trie.slidingContains[Foo](testSentence, { f => f.a.toString }, 0)
		//		assert (result.size === 1)
		//		assert (result(0)(0).a === 1 && result(0)(1).a === 2)
		//	}

		test(s"branching works at every level for $name") {
			val trie = newTrie()
			trie.add("angela dorothea merkel", 0.5f)
			trie.add("angela dorothea kanzler", 0.4f)
			assert(!trie.contains("dorothea").asEntry)
			assertProb(trie.contains("angela dorothea merkel").prob == 0.5f)
			assertProb(trie.contains("angela dorothea kanzler").prob == 0.4f)
			assert(!trie.contains("angela dorothea").asEntry)
			assert(trie.contains("angela dorothea merkel").asEntry)
		}

		test(s"can go many levels deep for $name") {
			val trie = newTrie()
			trie.add("ab cd ef gh ij kl")
			assert(trie.contains("ab cd ef gh ij kl").asEntry)
		}

		test(s"findAllInWithProbs with $name") {
			newTrie() match {
				case trie: NewTrie =>
					trie.add("angela merkel", 0.1f)
					trie.add("test1", 0.2f)
					trie.add("test2", 0.3f)

					val testSentence = "This is a test1 with angela merkel and test2"
					val res = trie.findAllInWithProbs(testSentence)
					println(res.toList)
				case _ =>
			}

		}
	}
}
