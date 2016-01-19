package de.uni_potsdam.hpi.coheel

import breeze.linalg.{DenseVector, DenseMatrix}
import de.uni_potsdam.hpi.coheel.programs.{RandomWalkReduceGroup, ClassificationProgram}
import org.apache.commons.math3.linear.{ArrayRealVector, RealMatrixChangingVisitor, Array2DRowRealMatrix, OpenMapRealMatrix}
import org.jblas.DoubleMatrix
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalautils.TolerantNumerics

@RunWith(classOf[JUnitRunner])
class RandomWalkTest extends FunSuite {


	test("test") {
		val p = new RandomWalkReduceGroup
		val m = new DenseMatrix[Float](7, 7, Array(
			1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 0.0f, 1.0f,
			1.0f, 1.0f, 0.0f, 0.0f, 0.0f, 0.0f, 2.0f,
			0.0f, 1.0f, 1.0f, 1.0f, 0.0f, 0.0f, 0.0f,
			0.0f, 1.0f, 0.0f, 1.0f, 1.0f, 1.0f, 2.0f,
			0.0f, 0.0f, 0.0f, 1.0f, 1.0f, 0.0f, 2.0f,
			0.0f, 0.0f, 0.0f, 1.0f, 0.0f, 1.0f, 2.0f,
			0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 1.0f
		)).t
		assert(m.cols === 7)
		assert(m.rows === 7)
		assert(m(0, 2) === 1.0)

//		val rowSum = m.getData.map(_.sum)
		val rowSum = Array(6.0f, 4.0f, 3.0f, 6.0f, 4.0f, 4.0f, 1.0f)

		for (r <- 0 until m.rows) {
			for (c <- 0 until m.cols) {
				m(r, c) = m(r, c) / rowSum(r)
			}

		}
//		m.walkInOptimizedOrder(new RealMatrixChangingVisitor {
//			override def visit(row: Int, column: Int, value: Double): Double = {
//				value / rowSum(row)
//			}
//			override def start(rows: Int, columns: Int, startRow: Int, endRow: Int, startColumn: Int, endColumn: Int): Unit = {}
//			override def end(): Double = 0.0
//		})

//		println(m)

		val s = new DenseMatrix[Float](1, 7, Array(1.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f))

		// Turn on double tolerance
		implicit val floatEquality = TolerantNumerics.tolerantFloatEquality(0.0001f)

		val iteration1 = p.randomWalk(m.copy, s, 1).t
		assert(iteration1(0, 0) === 0.2916f)
		assert(iteration1(1, 0) === 0.1416f)
		assert(iteration1(5, 0) === 0.0f)

		val iteration2 = p.randomWalk(m.copy, s, 2).t
		assert(iteration2(0, 0) === 0.2214f)
		assert(iteration2(1, 0) === 0.1316f)
		assert(iteration2(5, 0) === 0.0200f)

		val iteration100 = p.randomWalk(m.copy, s, 100).t
		assert(iteration100(0, 0) === 0.1890f)
		assert(iteration100(1, 0) === 0.0577f)
		assert(iteration100(2, 0) === 0.0373f)
		assert(iteration100(3, 0) === 0.0570f)
		assert(iteration100(4, 0) === 0.0443f)
		assert(iteration100(5, 0) === 0.0103f)
		assert(iteration100(6, 0) === 0.6043f)

		assert(iteration1.toArray.sum === 1.0f)
		assert(iteration2.toArray.sum === 1.0f)
		assert(iteration100.toArray.sum === 1.0f)


	}
}
