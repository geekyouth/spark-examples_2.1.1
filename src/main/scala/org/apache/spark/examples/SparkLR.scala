// scalastyle:off println
package org.apache.spark.examples

import java.util.Random

import scala.math.exp

import breeze.linalg.{DenseVector, Vector}

import org.apache.spark.sql.SparkSession

/**
 * Logistic regression based classification.
 * Usage: SparkLR [slices]
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.ml.classification.LogisticRegression.
 */
object SparkLR {
	val N = 10000 // Number of data points
	val D = 10 // Number of dimensions
	val R = 0.7 // Scaling factor
	val ITERATIONS = 5
	val rand = new Random(42)
	
	case class DataPoint(x: Vector[Double], y: Double)
	
	def generateData: Array[DataPoint] = {
		def generatePoint(i: Int): DataPoint = {
			val y = if (i % 2 == 0) -1 else 1
			val x = DenseVector.fill(D) {rand.nextGaussian + y * R}
			DataPoint(x, y)
		}
		Array.tabulate(N)(generatePoint)
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
		
		val spark = SparkSession
			.builder
			.appName("SparkLR")
			.getOrCreate()
		
		val numSlices = if (args.length > 0) args(0).toInt else 2
		val points = spark.sparkContext.parallelize(generateData, numSlices).cache()
		
		// Initialize w to a random value
		var w = DenseVector.fill(D) {2 * rand.nextDouble - 1}
		println("Initial w: " + w)
		
		for (i <- 1 to ITERATIONS) {
			println("On iteration " + i)
			val gradient = points.map { p =>
				p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
			}.reduce(_ + _)
			w -= gradient
		}
		
		println("Final w: " + w)
		
		spark.stop()
	}
}

// scalastyle:on println
