package de.uni_potsdam.hpi.coheel.ml

import de.uni_potsdam.hpi.coheel.programs.DataClasses.{Info, Classifiable}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait SecondOrderFeatureFunction {
	def apply(in: Seq[Classifiable[_]])(field: Classifiable[_] => Double): Seq[Double]

}
object SecondOrderFeatures {

	/**
	 * The rank in an ordered list
	 */
	val rank: SecondOrderFeatureFunction = {
		new SecondOrderFeatureFunction {
			override def apply(in: Seq[Classifiable[_]])(field: (Classifiable[_]) => Double): Seq[Double] = {
				val ranks = new ArrayBuffer[Double](in.size)
				var lastValue = Double.NaN
				var rank = 0
				for (i <- 0 until in.size) {
					if (field(in(i)) != lastValue) {
						lastValue = field(in(i))
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
			override def apply(in: Seq[Classifiable[_]])(field: (Classifiable[_]) => Double): Seq[Double] = {
				val deltaTops = new ArrayBuffer[Double](in.size)
				val topValue = field(in.head)
				for (i <- 0 until in.size) {
					deltaTops += topValue - field(in(i))
				}
				deltaTops
			}
		}
	}

	val deltaSucc: SecondOrderFeatureFunction = {
		new SecondOrderFeatureFunction {
			override def apply(in: Seq[Classifiable[_]])(field: (Classifiable[_]) => Double): Seq[Double] = {
				val deltaSuccs = new ArrayBuffer[Double](in.size)
				val deltaSuccCache = mutable.Map[Double, Double]()
				for (i <- 0 until in.size) {
					deltaSuccCache.get(field(in(i))) match {
						case Some(delta) =>
							deltaSuccs += delta
						case None =>
							val currentVal = field(in(i))
							var deltaSucc = Double.NaN
							var j = i
							while (j < in.size && deltaSucc.isNaN) {
								if (field(in(j)) != currentVal)
									deltaSucc = currentVal - field(in(j))
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
