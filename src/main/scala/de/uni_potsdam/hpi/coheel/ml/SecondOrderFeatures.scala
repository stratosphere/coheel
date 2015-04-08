package de.uni_potsdam.hpi.coheel.ml

import de.uni_potsdam.hpi.coheel.programs.DataClasses.LinkCandidate

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait SecondOrderFeatureFunction {
	def apply(in: Seq[LinkCandidate]): Seq[Double]

}
object SecondOrderFeatures {

	/**
	 * The rank in an ordered list
	 */
	val rank: SecondOrderFeatureFunction = {
		new SecondOrderFeatureFunction {
			override def apply(in: Seq[LinkCandidate]): Seq[Double] = {
				val ranks = new ArrayBuffer[Double](in.size)
				var lastValue = Double.NaN
				var rank = 0
				for (i <- 0 until in.size) {
					if (in(i).prob != lastValue) {
						lastValue = in(i).prob
						rank += 1
					}
					ranks += rank
				}
				ranks
			}
		}
	}

	/**
	 * Decrease in score in comparison to the highest scored entity
	 */
	val deltaTop: SecondOrderFeatureFunction = {
		new SecondOrderFeatureFunction {
			override def apply(in: Seq[LinkCandidate]): Seq[Double] = {
				val deltaTops = new ArrayBuffer[Double](in.size)
				val topValue = in.head.prob
				for (i <- 0 until in.size) {
					deltaTops += topValue - in(i).prob
				}
				deltaTops
			}
		}
	}

	val deltaSucc: SecondOrderFeatureFunction = {
		new SecondOrderFeatureFunction {
			override def apply(in: Seq[LinkCandidate]): Seq[Double] = {
				val deltaSuccs = new ArrayBuffer[Double](in.size)
				val deltaSuccCache = mutable.Map[Double, Double]()
				for (i <- 0 until in.size) {
					deltaSuccCache.get(in(i).prob) match {
						case Some(deltaSucc) =>
							deltaSuccs += deltaSucc
						case None =>
							val currentVal = in(i).prob
							var deltaSucc = Double.NaN
							var j = i
							while (j < in.size && deltaSucc.isNaN) {
								if (in(j).prob != currentVal)
									deltaSucc = currentVal - in(j).prob
								j += 1
							}
							deltaSuccs += deltaSucc
					}

				}

				deltaSuccs
			}
		}
	}
}
