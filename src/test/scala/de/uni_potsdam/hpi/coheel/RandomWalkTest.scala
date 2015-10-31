package de.uni_potsdam.hpi.coheel

import de.uni_potsdam.hpi.coheel.programs.ClassificationProgram
import org.apache.commons.math3.linear.{ArrayRealVector, RealMatrixChangingVisitor, Array2DRowRealMatrix, OpenMapRealMatrix}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalautils.TolerantNumerics

@RunWith(classOf[JUnitRunner])
class RandomWalkTest extends FunSuite {


	test("test") {
		val p = new ClassificationProgram
		val m = new Array2DRowRealMatrix(Array(
			Array(1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0),
			Array(1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 2.0),
			Array(0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0),
			Array(0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 2.0),
			Array(0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 2.0),
			Array(0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 2.0),
			Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0)
		))
		assert(m.getColumnDimension === 7)
		assert(m.getRowDimension === 7)

		val rowSum = m.getData.map(_.sum)
		println(rowSum.deep)
		println()

		m.walkInOptimizedOrder(new RealMatrixChangingVisitor {
			override def visit(row: Int, column: Int, value: Double): Double = {
				value / rowSum(row)
			}
			override def start(rows: Int, columns: Int, startRow: Int, endRow: Int, startColumn: Int, endColumn: Int): Unit = {}
			override def end(): Double = 0.0
		})

		m.getDataRef.foreach { a => println(a.deep) }
		println()

		val s = new ArrayRealVector(Array(1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))

		// Turn on double tolerance
		implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.001)

		val iteration1 = p.randomWalk(m, s, 1)
		assert(iteration1.getEntry(0) === 0.2916)
		assert(iteration1.getEntry(1) === 0.1416)
		assert(iteration1.getEntry(5) === 0.0)

		val iteration2 = p.randomWalk(m, s, 2)
		assert(iteration2.getEntry(0) === 0.2214)
		assert(iteration2.getEntry(1) === 0.1316)
		assert(iteration2.getEntry(5) === 0.0200)

		val iteration100 = p.randomWalk(m, s, 100)
		assert(iteration100.getEntry(0) === 0.1890)
		assert(iteration100.getEntry(1) === 0.0577)
		assert(iteration100.getEntry(2) === 0.0373)
		assert(iteration100.getEntry(3) === 0.0570)
		assert(iteration100.getEntry(4) === 0.0443)
		assert(iteration100.getEntry(5) === 0.0103)
		assert(iteration100.getEntry(6) === 0.6043)

		assert(iteration1.toArray.sum === 1.0)
		assert(iteration2.toArray.sum === 1.0)
		assert(iteration100.toArray.sum === 1.0)


	}
}
