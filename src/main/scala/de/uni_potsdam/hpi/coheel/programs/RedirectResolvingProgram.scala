package de.uni_potsdam.hpi.coheel.programs

import OutputFiles._
import org.apache.flink.api.common.{Plan, ProgramDescription, Program}
import org.apache.flink.api.scala.operators.CsvOutputFormat
import org.apache.flink.api.scala.{ScalaPlan, DataSet, TextFile}

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
			val resolvedRedirects = redirects.join(ws)
				.where { case Redirect(from, to) => from }
				.isEqualTo { case ContextLink(from, origTo, to) => to }
				.map { case (redirect, contextLink) =>
					ContextLink(contextLink.from, contextLink.origTo, redirect.to)
				}
			var shownDelimiter = false
			val result = s.join(resolvedRedirects)
				.where { cl => (cl.from, cl.origTo) }
				.isEqualTo { cl => (cl.from, cl.origTo) }
				.map { (orig, resolved) =>
					resolved
				}.filter { resolved =>
					if (!shownDelimiter) {
						println("#########NEXT ITERATION###########")
						shownDelimiter = true
					}
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
