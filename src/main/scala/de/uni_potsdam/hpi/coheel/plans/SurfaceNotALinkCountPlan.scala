package de.uni_potsdam.hpi.coheel.plans

import eu.stratosphere.api.common.{Program, ProgramDescription, Plan}

class SurfaceNotALinkCountPlan extends Program with ProgramDescription {

	override def getDescription = "Training the model parameters for CohEEL."

	override def getPlan(args: String*): Plan = {
		null
	}
}
