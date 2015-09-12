package de.uni_potsdam.hpi.coheel.graphs

import org.apache.commons.math3.linear.{RealMatrixChangingVisitor, RealMatrix, MatrixUtils}

object MatrixPlayground {

	def main(args: Array[String]): Unit = {
		val matrixData = Array(
			Array(0.0, 0.0, 0.0, 0.0, 1.0),
			Array(1.0, 0.0, 0.0, 0.0, 0.0),
			Array(0.0, 1.0, 0.0, 0.0, 0.0),
			Array(1.0, 0.0, 1.0, 0.0, 0.0),
			Array(0.0, 1.0, 1.0, 1.0, 0.0)
		)
		def buildTransiitionMatrix(matrix: RealMatrix, restartProb: Double = 0.25): RealMatrix = {
			val nrElements = matrix.getData.length
			val transitionMatrix = MatrixUtils.createRealMatrix(nrElements, nrElements)
			val rowCount = new Array[Double](nrElements)
			matrix.walkInOptimizedOrder(new RealMatrixChangingVisitor {
				override def visit(r: Int, c: Int, v: Double): Double = {
					rowCount(r) += v
					v
				}
				override def start(i: Int, i1: Int, i2: Int, i3: Int, i4: Int, i5: Int): Unit = {}
				override def end(): Double = { 0.0 }
			})
			val elRestartProb = restartProb / nrElements
			val restProb = 1.0 - restartProb
			matrix.walkInOptimizedOrder(new RealMatrixChangingVisitor {
				override def visit(r: Int, c: Int, v: Double): Double = {
					elRestartProb + restProb * v / rowCount(r)
				}
				override def start(i: Int, i1: Int, i2: Int, i3: Int, i4: Int, i5: Int): Unit = {}
				override def end(): Double = { 0.0 }
			})
			transitionMatrix
		}
		val matrix = MatrixUtils.createRealMatrix(matrixData)
		println(matrix)
		val transitionMatrix = buildTransiitionMatrix(matrix)
		println(matrix)
		println(transitionMatrix)
	}

}
