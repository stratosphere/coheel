package de.uni_potsdam.hpi.coheel.datastructures

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class TrieTest extends FunSuite {

	buildTests(new HashTrie())
//	buildTests(new PatriciaTrieWrapper())
//	buildTests(new ConcurrentTreesTrie())
	buildTests(new TrieToni())
	buildTests(new NewTrie())

	def buildTests[T <: Trie](trie: => Trie): Unit = {
		def newTrie(): Trie = {
			trie
		}
		val name = trie.getClass.getSimpleName

		test(s"single word queries work for $name") {
			val trie = newTrie()
			trie.add("angela")
			assert(trie.contains("angela").asEntry)
		}

		test(s"later add's in middle work for $name") {
			val trie = newTrie()
			trie.add("angela merkel")
			trie.add("angela")
			assert(trie.contains("angela").asEntry)
			assert(trie.contains("angela merkel").asEntry)
		}

		test(s"add reuses existing nodes for $name") {
			val trie = newTrie()
			trie.add("angela merkel")
			trie.add("angela dorothea")
			val testSentence = "angela dorothea"
			val result = trie.findAllIn(testSentence).toList
			assert (result.size === 1)
		}

		test(s"contains handles spaces #1 for $name") {
			val trie = newTrie()
			trie.add("angela")
			val testSentence = "angela            merkel"
			val result = trie.findAllIn(testSentence).toList
			assert (result.size === 1)
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
			trie.add("angela merkel")
			assert(trie.contains("angela merkel").asEntry)
		}

		test(s"multiple word queries #2 work for $name") {
			val trie = newTrie()
			trie.add("angela merkel chancellor")
			trie.add("angela merkel")
			val testSentence = "angela merkel"
			val result = trie.findAllIn(testSentence).toList
			assert (result.size === 1)
		}

		test(s"distinction between contains-asEntry and contains-asIntermediateNode for $name") {
			val trie = newTrie()
			trie.add("angela dorothea merkel")

			assert(trie.contains("angela").asIntermediateNode)
			assert(trie.contains("angela dorothea").asIntermediateNode)
			assert(trie.contains("angela dorothea merkel").asIntermediateNode)

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
			trie.add("angela merkel")
			trie.add("angela merkel")
			trie.add("angela merkel")
			assert(!trie.contains("angela").asEntry)
			assert(trie.contains("angela merkel").asEntry)
		}

		test(s"findAllIn finds all occurrences for $name") {
			val trie = newTrie()
			trie.add("angela merkel")
			trie.add("angela merkel is german")
			trie.add("angela")
			trie.add("merkel")

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
			trie.add("angela")
			trie.add("angela dorothea merkel")

			val testSentence = "angela"
			val result = trie.findAllIn(testSentence).toList
			assert (result.size === 1)
		}

		test(s"findAllIn finds all occurrences of substrings for $name") {
			val trie = newTrie()
			trie.add("angela merkel")
			trie.add("chancellor schroeder")
			trie.add("chancellor angela merkel")

			val testSentence = "chancellor angela merkel"
			val result = trie.findAllIn(testSentence).toList
			assert (result.size === 2)
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
			trie.add("angela dorothea merkel")
			trie.add("angela dorothea kanzler")
			assert(!trie.contains("dorothea").asEntry)
			assert(!trie.contains("angela dorothea").asEntry)
			assert(trie.contains("angela dorothea merkel").asEntry)
		}

		test(s"can go many levels deep for $name") {
			val trie = newTrie()
			trie.add("ab cd ef gh ij kl")
			assert(trie.contains("ab cd ef gh ij kl").asEntry)
		}
	}
}
