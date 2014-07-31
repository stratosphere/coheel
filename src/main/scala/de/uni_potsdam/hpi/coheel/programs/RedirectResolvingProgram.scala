package de.uni_potsdam.hpi.coheel.programs

import eu.stratosphere.api.common.{Program, Plan, ProgramDescription}
import eu.stratosphere.api.scala.{TextFile, ScalaPlan, DataSource}
import OutputFiles._
import eu.stratosphere.api.scala.operators.CsvOutputFormat

class RedirectResolvingProgram extends Program with ProgramDescription {

	override def getDescription = "Counting how often a surface occurs, but not as a link."

	override def getPlan(args: String*): Plan = {

		val redirects    = DataSource(redirectPath, textInput)
		val contextLinks = TextFile(contextLinkProbsPath).map { line =>
			val split = line.split('\t')
			(split(0), split(1))
		}


		val resolvedRedirects = contextLinks.join(redirects)
			.where { case (from, to) => to }
			.isEqualTo { case (from, to) => from }
			.map { case (contextLink, redirect) =>
				redirect
			}

		val surfaceLinkOccurrenceOutput = resolvedRedirects.write(redirectResolvPath,
			CsvOutputFormat[(String, String)]("\n", "\t"))
		val plan = new ScalaPlan(Seq(surfaceLinkOccurrenceOutput))
		plan
	}
}
