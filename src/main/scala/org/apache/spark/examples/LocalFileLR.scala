// scalastyle:off println
package org.apache.spark.examples

import java.util.Random

import breeze.linalg.{DenseVector, Vector}

/**
 * Logistic regression based classification.
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.ml.classification.LogisticRegression.
 */
object LocalFileLR {
	val D = 10 // Number of dimensions
	val rand = new Random(42)
	
	case class DataPoint(x: Vector[Double], y: Double)
	
	def parsePoint(line: String): DataPoint = {
		val nums = line.split(' ').map(_.toDouble)
		DataPoint(new DenseVector(nums.slice(1, D + 1)), nums(0))
	}
	
	def showWarning() {
		System.err.println(
			"""WARN: This is a naive implementation of Logistic Regression and is given as an example!
				|Please use org.apache.spark.ml.classification.LogisticRegression
				|for more conventional use.
      """.stripMargin)
	}
	
	def main(args: Array[String]) {
		
		showWarning()
		
		val lines = scala.io.Source.fromFile(args(0)).getLines().toArray
		val points = lines.map(parsePoint _)
		val ITERATIONS = args(1).toInt
		
		// Initialize w to a random value
		var w = DenseVector.fill(D) {2 * rand.nextDouble - 1}
		println("Initial w: " + w)
		
		for (i <- 1 to ITERATIONS) {
			println("On iteration " + i)
			var gradient = DenseVector.zeros[Double](D)
			for (p <- points) {
				val scale = (1 / (1 + math.exp(-p.y * (w.dot(p.x)))) - 1) * p.y
				gradient += p.x * scale
			}
			w -= gradient
		}
		
		println("Final w: " + w)
	}
}

// scalastyle:on println
