package de.uni_potsdam.hpi.coheel

import de.uni_potsdam.hpi.coheel.io.Sample
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper

@RunWith(classOf[JUnitRunner])
class TokenizerTest extends FunSuite {

	val text = "Toynbee's law of challenge and response"

	test("Test tokenizer") {
		println("Raw text")
		println(text)
		println("Tokenized text with 1x stemming")
		println(TokenizerHelper.tokenize(text).mkString(" "))
		println("Tokenized text with 2x stemming")
		println(TokenizerHelper.tokenize(TokenizerHelper.tokenize(text).mkString(" ")).mkString(" "))
	}
}
