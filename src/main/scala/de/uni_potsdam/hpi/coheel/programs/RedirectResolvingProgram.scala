package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.io.OutputFiles
import OutputFiles._
import org.apache.flink.api.common.ProgramDescription
import org.apache.flink.api.scala._
import org.apache.log4j.Logger

class RedirectResolvingProgram extends CoheelProgram with ProgramDescription {

	val log = Logger.getLogger(getClass)

	case class ContextLink(from: String, origTo: String, to: String)
	case class Redirect(from: String, to: String)
	override def getDescription = "Resolving redirects"

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val redirects    = env.readTextFile(redirectPath).map { line =>
			val split = line.split('\t')
			Redirect(split(0), split(1))
		}.name("Redirects")

		val contextLinks = env.readTextFile(contextLinkProbsPath).map { line =>
			val split = line.split('\t')
			ContextLink(split(0), split(1), split(1))
		}.name("Context-Links")

		def iterate(s: DataSet[ContextLink], ws: DataSet[ContextLink]): (DataSet[ContextLink], DataSet[ContextLink]) = {
			val resolvedRedirects = redirects.join(ws)
				.where { _.from }
				.equalTo { _.to }
				.map { joinResult => joinResult match {
					case (redirect, contextLink) =>
						val cl = ContextLink(contextLink.from, contextLink.origTo, redirect.to)
						log.info(cl.toString)
						cl
				}
			}.name("Resolved-Redirects-From-Iteration")

			val result = s.join(resolvedRedirects)
				.where { cl => (cl.from, cl.origTo) }
				.equalTo { cl => (cl.from, cl.origTo) }
				.map { joinResult => joinResult match {
					case (orig, resolved) => resolved
				}
			}.name("Useless-Join-Still-Resolved-Redirects-From-Iteration")
			(result, result)
		}

		val resolvedRedirects = contextLinks
			.iterateDelta(contextLinks, 10, Array("from", "origTo")) { (solution, workset) =>
			iterate(solution, workset)
		}
			.name("Resolved-Redirects")
			.map { cl => (cl.from, cl.to) }
			.name("Final-Redirect-Result")

		resolvedRedirects.writeAsTsv(resolvedRedirectsPath)
	}
}
