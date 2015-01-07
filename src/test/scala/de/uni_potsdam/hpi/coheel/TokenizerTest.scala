package de.uni_potsdam.hpi.coheel

import de.uni_potsdam.hpi.coheel.io.Sample
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import de.uni_potsdam.hpi.coheel.wiki.TokenizerHelper

@RunWith(classOf[JUnitRunner])
class TokenizerTest extends FunSuite {

	val text = Sample.ANGELA_MERKEL_SAMPLE_TEXT

	test("Test tokenizer") {
		println("Raw text")
		println(text)
		println("Tokenized text, no stemming")
		println(TokenizerHelper.tokenize(text, stemming = false).mkString(" "))
		println("Tokenized text, with stemming")
		println(TokenizerHelper.tokenize(text, stemming = true).mkString(" "))
	}
}
