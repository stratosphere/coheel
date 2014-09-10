package de.uni_potsdam.hpi.coheel.programs

import OutputFiles._
import org.apache.flink.api.common.{Plan, ProgramDescription, Program}
import org.apache.flink.api.scala.operators.CsvOutputFormat
import org.apache.flink.api.scala.{ScalaPlan, DataSet, TextFile}
import DataSetNaming._
import org.slf4s.Logging

class RedirectResolvingProgram extends Program with ProgramDescription with Logging {

	case class ContextLink(from: String, origTo: String, to: String)
	case class Redirect(from: String, to: String)
	override def getDescription = "Resolving redirects"

	override def getPlan(args: String*): Plan = {

		val redirects    = TextFile(redirectPath).map { line =>
			val split = line.split('\t')
			Redirect(split(0), split(1))
		}.name("Redirects")

		val contextLinks = TextFile(contextLinkProbsPath).map { line =>
			val split = line.split('\t')
			ContextLink(split(0), split(1), split(1))
		}.name("Context-Links")

		def iterate(s: DataSet[ContextLink], ws: DataSet[ContextLink]): (DataSet[ContextLink], DataSet[ContextLink]) = {
			val resolvedRedirects = redirects.join(ws)
				.where { case Redirect(from, to) => from }
				.isEqualTo { case ContextLink(from, origTo, to) => to }
				.map { case (redirect, contextLink) =>
					val cl = ContextLink(contextLink.from, contextLink.origTo, redirect.to)
					log.info(cl.toString)
					cl
				}.name("Resolved-Redirects-From-Iteration")
			var shownDelimiter = false
			val result = s.join(resolvedRedirects)
				.where { cl => (cl.from, cl.origTo) }
				.isEqualTo { cl => (cl.from, cl.origTo) }
				.map { (orig, resolved) =>
					resolved
				}.name("Useless-Join-Still-Resolved-Redirects-From-Iteration")
			(result, result)
		}
		val resolvedRedirects = contextLinks
			.iterateWithDelta(contextLinks, { cl => (cl.from, cl.origTo) }, iterate, 10)
			.name("Resolved-Redirects")
			.map { cl => (cl.from, cl.to) }
			.name("Final-Redirect-Result")

		val surfaceLinkOccurrenceOutput = resolvedRedirects.write(resolvedRedirectsPath,
			CsvOutputFormat[(String, String)]("\n", "\t"))
		val plan = new ScalaPlan(Seq(surfaceLinkOccurrenceOutput))
		plan
	}
}
