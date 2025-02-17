// scalastyle:off println
package org.apache.spark.examples

import java.util.Random

import scala.math.exp

import breeze.linalg.{DenseVector, Vector}

import org.apache.spark.sql.SparkSession

/**
 * Logistic regression based classification.
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.ml.classification.LogisticRegression.
 */
object SparkHdfsLR {
	val D = 10 // Number of dimensions
	val rand = new Random(42)
	
	case class DataPoint(x: Vector[Double], y: Double)
	
	def parsePoint(line: String): DataPoint = {
		val tok = new java.util.StringTokenizer(line, " ")
		var y = tok.nextToken.toDouble
		var x = new Array[Double](D)
		var i = 0
		while (i < D) {
			x(i) = tok.nextToken.toDouble;
			i += 1
		}
		DataPoint(new DenseVector(x), y)
	}
	
	def showWarning() {
		System.err.println(
			"""WARN: This is a naive implementation of Logistic Regression and is given as an example!
				|Please use org.apache.spark.ml.classification.LogisticRegression
				|for more conventional use.
      """.stripMargin)
	}
	
	def main(args: Array[String]) {
		
		if (args.length < 2) {
			System.err.println("Usage: SparkHdfsLR <file> <iters>")
			System.exit(1)
		}
		
		showWarning()
		
		val spark = SparkSession
			.builder
			.appName("SparkHdfsLR")
			.getOrCreate()
		
		val inputPath = args(0)
		val lines = spark.read.textFile(inputPath).rdd
		
		val points = lines.map(parsePoint).cache()
		val ITERATIONS = args(1).toInt
		
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
