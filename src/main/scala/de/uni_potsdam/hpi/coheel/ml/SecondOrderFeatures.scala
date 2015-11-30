package de.uni_potsdam.hpi.coheel.ml

import de.uni_potsdam.hpi.coheel.programs.DataClasses.{Info, Classifiable}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait SecondOrderFeatureFunction {
	def apply(in: Seq[(Classifiable[_], Int)])(field: Classifiable[_] => Double): Seq[Double]

}
object SecondOrderFeatures {

	/**
	 * The rank in an ordered list
	 */
	val rank: SecondOrderFeatureFunction = {
		new SecondOrderFeatureFunction {
			override def apply(in: Seq[(Classifiable[_], Int)])(field: (Classifiable[_]) => Double): Seq[Double] = {
				val ranks = new Array[Double](in.size)
				var lastValue = Double.NaN
				var rank = 0
				for (i <- 0 until in.size) {
					if (field(in(i)._1) != lastValue) {
						lastValue = field(in(i)._1)
						rank += 1
					}
					ranks(in(i)._2) = rank
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
			override def apply(in: Seq[(Classifiable[_], Int)])(field: (Classifiable[_]) => Double): Seq[Double] = {
				val deltaTops = new Array[Double](in.size)
				val topValue = field(in.head._1)
				for (i <- 0 until in.size) {
					deltaTops(in(i)._2) = topValue - field(in(i)._1)
				}
				deltaTops
			}
		}
	}

	val deltaSucc: SecondOrderFeatureFunction = {
		new SecondOrderFeatureFunction {
			override def apply(in: Seq[(Classifiable[_], Int)])(field: (Classifiable[_]) => Double): Seq[Double] = {
				val deltaSuccs = new Array[Double](in.size)
				for (i <- 0 until in.size) {
					val currentVal = field(in(i)._1)
					var deltaSucc = Double.NaN
					var j = i
					while (j < in.size && deltaSucc.isNaN) {
						if (field(in(j)._1) != currentVal)
							deltaSucc = currentVal - field(in(j)._1)
						j += 1
					}
					deltaSuccs(in(i)._2) = deltaSucc
				}
				deltaSuccs
			}
		}
	}
}
