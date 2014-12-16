package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.programs.DataClasses.{ContextLink, Redirect}
import org.apache.flink.api.common.ProgramDescription
import org.apache.flink.api.scala._
import org.apache.log4j.Logger

class RedirectResolvingProgram extends CoheelProgram {

	@transient val log = Logger.getLogger(getClass)

	override def getDescription = "Resolving redirects"

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val redirects    = env.readTextFile(redirectPath).map { line =>
			val split = line.split('\t')
			Redirect(split(0), split(1))
		}.name("Redirects")

		val contextLinks = env.readTextFile(contextLinkProbsPath).map { line =>
			val split = line.split('\t')
			ContextLink(split(0), split(1), split(1), split(2).toDouble)
		}.name("Context-Links")

		def iterate(s: DataSet[ContextLink], ws: DataSet[ContextLink]): (DataSet[ContextLink], DataSet[ContextLink]) = {
			val resolvedRedirects = redirects.join(ws)
				.where { _.from }
				.equalTo { _.to }
				.map { joinResult =>
					joinResult match {
					case (redirect, contextLink) =>
						val cl = contextLink.copy(to = redirect.to)
						println(cl.toString)
						cl
				}
			}.name("Resolved-Redirects-From-Iteration")
			(resolvedRedirects, resolvedRedirects)
		}

		// resolve redirects via delta iteration
		val resolvedRedirects = contextLinks
			.iterateDelta(contextLinks, 4, Array("from", "origTo"))(iterate)
		.name("Resolved-Redirects")
		.map { cl => (cl.from, cl.to, cl.prob) }
		.name("Final-Redirect-Result")

		resolvedRedirects.writeAsTsv(resolvedRedirectsPath)
	}
}
