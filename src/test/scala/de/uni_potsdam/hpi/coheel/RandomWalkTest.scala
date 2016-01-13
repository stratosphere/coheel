package de.uni_potsdam.hpi.coheel

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
		val m = new DoubleMatrix(Array(
			Array(1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0),
			Array(1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 2.0),
			Array(0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0),
			Array(0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 2.0),
			Array(0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 2.0),
			Array(0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 2.0),
			Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0)
		))
		assert(m.columns === 7)
		assert(m.rows === 7)

//		val rowSum = m.getData.map(_.sum)
		val rowSum = m.rowSums()
//		println(rowSum)
		println()

		for (r <- 0 until m.rows) {
			for (c <- 0 until m.columns) {
				m.put(r, c, m.get(r, c) / rowSum.get(r))
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

		val s = new DoubleMatrix(Array(Array(1.0), Array(0.0), Array(0.0), Array(0.0), Array(0.0), Array(0.0), Array(0.0)))

		// Turn on double tolerance
		implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.001)

		val iteration1 = p.randomWalk(m, s, 1)
		assert(iteration1.get(0) === 0.2916)
		assert(iteration1.get(1) === 0.1416)
		assert(iteration1.get(5) === 0.0)

		val iteration2 = p.randomWalk(m, s, 2)
		assert(iteration2.get(0) === 0.2214)
		assert(iteration2.get(1) === 0.1316)
		assert(iteration2.get(5) === 0.0200)

		val iteration100 = p.randomWalk(m, s, 100)
		assert(iteration100.get(0) === 0.1890)
		assert(iteration100.get(1) === 0.0577)
		assert(iteration100.get(2) === 0.0373)
		assert(iteration100.get(3) === 0.0570)
		assert(iteration100.get(4) === 0.0443)
		assert(iteration100.get(5) === 0.0103)
		assert(iteration100.get(6) === 0.6043)

		assert(iteration1.toArray.sum === 1.0)
		assert(iteration2.toArray.sum === 1.0)
		assert(iteration100.toArray.sum === 1.0)


	}
}
