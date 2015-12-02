package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.programs.DataClasses.{ContextLinkWithOrig, Redirect}
import org.apache.flink.api.common.functions.RichJoinFunction
import org.apache.flink.api.scala._

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

		def iterate(ds: DataSet[ContextLinkWithOrig]): DataSet[ContextLinkWithOrig] = {
			val resolvedRedirects = ds.leftOuterJoin(redirects)
				.where { _.to }
				.equalTo { _.from }
				.apply(new RichJoinFunction[ContextLinkWithOrig, Redirect, ContextLinkWithOrig] {
					override def join(contextLink: ContextLinkWithOrig, redirect: Redirect): ContextLinkWithOrig = {
						if (redirect == null)
							contextLink
						else
							contextLink.copy(to = redirect.to)
					}
				}).name("Resolved-Redirects-From-Iteration")
			resolvedRedirects
		}


		// resolve redirects via delta iteration
		val resolvedRedirects = contextLinks.iterate(3)(iterate)
			.map { cl => (cl.from, cl.to, cl.prob) }
			.name("Final-Redirect-Result")

		resolvedRedirects.writeAsTsv(resolvedRedirectsPath)
	}
}
