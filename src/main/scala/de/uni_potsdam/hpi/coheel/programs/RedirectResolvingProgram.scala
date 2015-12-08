package de.uni_potsdam.hpi.coheel.programs

import de.uni_potsdam.hpi.coheel.io.OutputFiles._
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import org.apache.flink.api.common.functions.RichJoinFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

class RedirectResolvingProgram extends NoParamCoheelProgram {

	override def getDescription = "Resolving redirects"

	override def buildProgram(env: ExecutionEnvironment): Unit = {
		val redirects    = env.readTextFile(redirectPath).map { line =>
			val split = line.split('\t')
			Redirect(split(0), split(1))
		}.name("Redirects")

		val surfaceProbs = readSurfaceProbs().map { surfaceProb =>
			import surfaceProb._
			SurfaceProbResolving(surface, destination, prob)
		}

		val contextLinks = env.readTextFile(contextLinkProbsPath).map { line =>
			val split = line.split('\t')
			ContextLinkResolving(split(0), split(1), split(2).toDouble)
		}.name("Context-Links")

		def iterate[T <: ThingToResolve[T] : TypeInformation : ClassTag](ds: DataSet[T]): DataSet[T] = {
			val resolvedRedirects = ds.leftOuterJoin(redirects)
				.where { _.to }
				.equalTo { _.from }
				.apply(new RichJoinFunction[T, Redirect, T] {
					override def join(contextLink: T, redirect: Redirect): T= {
						if (redirect == null)
							contextLink
						else
							contextLink.updateTo(redirect.to)
					}
				}).name("Resolved-Redirects-From-Iteration")
			resolvedRedirects
		}

		val resolvedContextLinks = contextLinks.iterate(3)(iterate)
			.groupBy("from", "to")
			.reduce { (cl1, cl2) =>
				cl1.copy(prob = cl1.prob + cl2.prob)
			}
			.map { cl => (cl.from, cl.to, cl.prob) }
			.name("Final-Resolved-Context-Links")

		val resolvedSurfaceProbs = surfaceProbs.iterate(3)(iterate)
			.groupBy("surface", "destination")
			.reduce { (sp1, sp2) =>
				sp1.copy(prob = sp1.prob + sp2.prob)
			}
			.name("Final-Resolved-Surface-Probs")

		resolvedContextLinks.writeAsTsv(contextLinkProbsResolvedPath)
		resolvedSurfaceProbs.writeAsTsv(surfaceProbsResolvedPath)
	}
}
