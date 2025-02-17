// scalastyle:off println
package org.apache.spark.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume._
import org.apache.spark.util.IntParam

/**
 * Produces a count of events received from Flume.
 *
 * This should be used in conjunction with an AvroSink in Flume. It will start
 * an Avro server on at the request host:port address and listen for requests.
 * Your Flume AvroSink should be pointed to this address.
 *
 * Usage: FlumeEventCount <host> <port>
 * <host> is the host the Flume receiver will be started on - a receiver
 * creates a server and listens for flume events.
 * <port> is the port the Flume receiver will listen on.
 *
 * To run this example:
 * `$ bin/run-example org.apache.spark.examples.streaming.FlumeEventCount <host> <port> `
 */
object FlumeEventCount {
	def main(args: Array[String]) {
		if (args.length < 2) {
			System.err.println(
				"Usage: FlumeEventCount <host> <port>")
			System.exit(1)
		}
		
		StreamingExamples.setStreamingLogLevels()
		
		val Array(host, IntParam(port)) = args
		
		val batchInterval = Milliseconds(2000)
		
		// Create the context and set the batch size
		val sparkConf = new SparkConf().setAppName("FlumeEventCount")
		val ssc = new StreamingContext(sparkConf, batchInterval)
		
		// Create a flume stream
		val stream = FlumeUtils.createStream(ssc, host, port, StorageLevel.MEMORY_ONLY_SER_2)
		
		// Print out the count of events received from this server in each batch
		stream.count().map(cnt => "Received " + cnt + " flume events.").print()
		
		ssc.start()
		ssc.awaitTermination()
	}
}

// scalastyle:on println
