package de.uni_potsdam.hpi.coheel

import de.uni_potsdam.hpi.coheel.util.Util
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class UtilTest extends FunSuite {

	val tokens = "This is a test of the context extraction".split(' ')

	test("context") {
		assert(Util.extractContext(tokens, 2, 1).get === Array("is", "a", "test"))
		assert(Util.extractContext(tokens, 0, 1).get === Array("This", "is", "a"))
		assert(Util.extractContext(tokens, 7, 3).get === Array("is", "a", "test", "of", "the", "context", "extraction"))
		assert(Util.extractContext(tokens, 7, 4) === None)
	}
}
