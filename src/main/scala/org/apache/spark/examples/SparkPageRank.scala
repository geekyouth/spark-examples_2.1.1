// scalastyle:off println
package org.apache.spark.examples

import org.apache.spark.sql.SparkSession

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.graphx.lib.PageRank
 *
 * Example Usage:
 * {{{
 * bin/run-example SparkPageRank data/mllib/pagerank_data.txt 10
 * }}}
 */
object SparkPageRank {
	
	def showWarning() {
		System.err.println(
			"""WARN: This is a naive implementation of PageRank and is given as an example!
				|Please use the PageRank implementation found in org.apache.spark.graphx.lib.PageRank
				|for more conventional use.
      """.stripMargin)
	}
	
	def main(args: Array[String]) {
		if (args.length < 1) {
			System.err.println("Usage: SparkPageRank <file> <iter>")
			System.exit(1)
		}
		
		showWarning()
		
		val spark = SparkSession
			.builder
			.appName("SparkPageRank")
			.getOrCreate()
		
		val iters = if (args.length > 1) args(1).toInt else 10
		val lines = spark.read.textFile(args(0)).rdd
		val links = lines.map { s =>
			val parts = s.split("\\s+")
			(parts(0), parts(1))
		}.distinct().groupByKey().cache()
		var ranks = links.mapValues(v => 1.0)
		
		for (i <- 1 to iters) {
			val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
				val size = urls.size
				urls.map(url => (url, rank / size))
			}
			ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
		}
		
		val output = ranks.collect()
		output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
		
		spark.stop()
	}
}

// scalastyle:on println
