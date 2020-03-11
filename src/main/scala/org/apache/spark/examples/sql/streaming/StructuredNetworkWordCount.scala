// scalastyle:off println
package org.apache.spark.examples.sql.streaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network.
 *
 * Usage: StructuredNetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Structured Streaming
 * would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 * and then run the example
 * `$ bin/run-example sql.streaming.StructuredNetworkWordCount
 * localhost 9999`
 */
object StructuredNetworkWordCount {
	def main(args: Array[String]) {
		if (args.length < 2) {
			System.err.println("Usage: StructuredNetworkWordCount <hostname> <port>")
			System.exit(1)
		}
		
		val host = args(0)
		val port = args(1).toInt
		
		val spark = SparkSession
			.builder
			.appName("StructuredNetworkWordCount")
			.getOrCreate()
		
		import spark.implicits._
		
		// Create DataFrame representing the stream of input lines from connection to host:port
		val lines = spark.readStream
			.format("socket")
			.option("host", host)
			.option("port", port)
			.load()
		
		// Split the lines into words
		val words = lines.as[String].flatMap(_.split(" "))
		
		// Generate running word count
		val wordCounts = words.groupBy("value").count()
		
		// Start running the query that prints the running counts to the console
		val query = wordCounts.writeStream
			.outputMode("complete")
			.format("console")
			.start()
		
		query.awaitTermination()
	}
}

// scalastyle:on println
