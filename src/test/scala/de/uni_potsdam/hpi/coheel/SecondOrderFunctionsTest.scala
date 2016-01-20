package de.uni_potsdam.hpi.coheel

import de.uni_potsdam.hpi.coheel.programs.DataClasses.{Classifiable, ClassificationInfo}
import de.uni_potsdam.hpi.coheel.programs.FeatureHelper
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SecondOrderFunctionsTest extends FunSuite {

	test("second order functions") {
		val classifiables = Seq[Classifiable[ClassificationInfo]](
			Classifiable("id0", "foo", Array(), "1", 0.25, 1.0, -500, ClassificationInfo(null, null, Array())),
			Classifiable("id0", "foo", Array(), "2", 0.50, 1.0, -750, ClassificationInfo(null, null, Array())),
			Classifiable("id0", "foo", Array(), "3", 0.30, 1.0, -400, ClassificationInfo(null, null, Array())),
			Classifiable("id0", "foo", Array(), "4", 0.05, 1.0, -800, ClassificationInfo(null, null, Array()))
		)
		FeatureHelper.applyCoheelFunctions(classifiables) { featureLine =>
			println(featureLine)
		}
	}
}
