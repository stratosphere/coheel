package de.uni_potsdam.hpi.coheel.graphs

import java.io.{FileReader, BufferedReader, FileWriter, BufferedWriter}

object RocBuilder {

	def main(args: Array[String]): Unit = {
		buildRocPlot()
	}

	def buildRocPlot(): Unit = {
		val bw = new BufferedWriter(new FileWriter("plots/roc.csv", false))
		(0 to 4400 by 250).foreach { threshold =>
			var tp = 0
			var fp = 0
			var fn = 0
			var tn = 0

			val br = new BufferedReader(new FileReader("testoutput/possible-surface-occurrences.wiki"))
			var line: String = br.readLine()
			while (line != null) {
				val split = line.split('\t')
				val actual = split(2).toInt
				val possible = split(3).toInt
				if (possible > threshold) {
					fn += actual
					tn += (possible - actual)
				} else {
					tp += actual
					fp += (possible - actual)
				}
				line = br.readLine()
			}

			val tpr = tp.toDouble / (tp + fn).toDouble
			val fpr = fp.toDouble / (fp + tn).toDouble

			bw.write(s"$fpr,$tpr,$threshold,$tp,$tn,$fp,$fn")
			bw.newLine()
		}
		bw.close()
	}
}
