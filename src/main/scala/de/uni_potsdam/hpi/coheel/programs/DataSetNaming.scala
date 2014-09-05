package de.uni_potsdam.hpi.coheel.programs

import eu.stratosphere.api.scala.DataSet

class DataSetWithName[T](ds: DataSet[T]) {

	def name(name: String): DataSet[T] = {
		ds.contract.setName(name)
		ds
	}

}
object DataSetNaming {

	implicit def toDataSetWithName[T](ds: DataSet[T]): DataSetWithName[T] = {
		new DataSetWithName[T](ds)
	}

}
