package de.uni_potsdam.hpi.coheel.programs

import eu.stratosphere.api.common.{Program, Plan, ProgramDescription}
import eu.stratosphere.api.scala.{DataSet, TextFile, ScalaPlan, DataSource}
import OutputFiles._
import eu.stratosphere.api.scala.operators.CsvOutputFormat

class RedirectResolvingProgram extends Program with ProgramDescription {

	case class ContextLink(from: String, origTo: String, to: String)
	case class Redirect(from: String, to: String)
	override def getDescription = "Resolving redirects"

	override def getPlan(args: String*): Plan = {

		val redirects    = TextFile(redirectPath).map { line =>
			val split = line.split('\t')
			Redirect(split(0), split(1))
		}
		val contextLinks = TextFile(contextLinkProbsPath).map { line =>
			val split = line.split('\t')
			ContextLink(split(0), split(1), split(1))
		}

		def iterate(s: DataSet[ContextLink], ws: DataSet[ContextLink]): (DataSet[ContextLink], DataSet[ContextLink]) = {
			val resolvedRedirects = ws.filter { input =>
					true
				}.join(redirects)
				.where { case ContextLink(from, origTo, to) => to }
				.isEqualTo { case Redirect(from, to) => from }
				.map { case (contextLink, redirect) =>
					ContextLink(contextLink.from, contextLink.origTo, redirect.to)
				}
			val result = s.join(resolvedRedirects)
				.where { cl => (cl.from, cl.origTo) }
				.isEqualTo { cl => (cl.from, cl.origTo) }
				.map { (orig, resolved) =>
					resolved
				}.filter { resolved =>
					println("Resolved: " + resolved)
					true
				}
			(result, result)
		}
		val resolvedRedirects = contextLinks
			.iterateWithDelta(contextLinks, { cl => (cl.from, cl.origTo) }, iterate, 10)
			.map { cl => (cl.from, cl.to) }

		val surfaceLinkOccurrenceOutput = resolvedRedirects.write(redirectResolvPath,
			CsvOutputFormat[(String, String)]("\n", "\t"))
		val plan = new ScalaPlan(Seq(surfaceLinkOccurrenceOutput))
		plan
	}
}
