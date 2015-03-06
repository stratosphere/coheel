package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.programs.DataClasses.{ContextLinkWithOrig, Redirect}
import org.apache.flink.api.common.ProgramDescription
import org.apache.flink.api.scala._
import org.apache.log4j.Logger

class RedirectResolvingProgram extends NoParamCoheelProgram {

	override def getDescription = "Resolving redirects"

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val redirects    = env.readTextFile(redirectPath).map { line =>
			val split = line.split('\t')
			Redirect(split(0), split(1))
		}.name("Redirects")

		val contextLinks = env.readTextFile(contextLinkProbsPath).map { line =>
			val split = line.split('\t')
			ContextLinkWithOrig(split(0), split(1), split(1), split(2).toDouble)
		}.name("Context-Links")

		def iterate(s: DataSet[ContextLinkWithOrig], ws: DataSet[ContextLinkWithOrig]): (DataSet[ContextLinkWithOrig], DataSet[ContextLinkWithOrig]) = {
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
