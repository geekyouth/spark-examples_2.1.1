// scalastyle:off println
package org.apache.spark.examples

import scala.collection.mutable
import scala.util.Random

import org.apache.spark.sql.SparkSession

/**
 * Transitive closure on a graph.
 */
object SparkTC {
	val numEdges = 200
	val numVertices = 100
	val rand = new Random(42)
	
	def generateGraph: Seq[(Int, Int)] = {
		val edges: mutable.Set[(Int, Int)] = mutable.Set.empty
		while (edges.size < numEdges) {
			val from = rand.nextInt(numVertices)
			val to = rand.nextInt(numVertices)
			if (from != to) edges.+=((from, to))
		}
		edges.toSeq
	}
	
	def main(args: Array[String]) {
		val spark = SparkSession
			.builder
			.appName("SparkTC")
			.getOrCreate()
		val slices = if (args.length > 0) args(0).toInt else 2
		var tc = spark.sparkContext.parallelize(generateGraph, slices).cache()
		
		// Linear transitive closure: each round grows paths by one edge,
		// by joining the graph's edges with the already-discovered paths.
		// e.g. join the path (y, z) from the TC with the edge (x, y) from
		// the graph to obtain the path (x, z).
		
		// Because join() joins on keys, the edges are stored in reversed order.
		val edges = tc.map(x => (x._2, x._1))
		
		// This join is iterated until a fixed point is reached.
		var oldCount = 0L
		var nextCount = tc.count()
		do {
			oldCount = nextCount
			// Perform the join, obtaining an RDD of (y, (z, x)) pairs,
			// then project the result to obtain the new (x, z) paths.
			tc = tc.union(tc.join(edges).map(x => (x._2._2, x._2._1))).distinct().cache()
			nextCount = tc.count()
		} while (nextCount != oldCount)
		
		println("TC has " + tc.count() + " edges.")
		spark.stop()
	}
}

// scalastyle:on println
