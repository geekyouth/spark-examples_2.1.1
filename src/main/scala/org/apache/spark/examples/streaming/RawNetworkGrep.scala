// scalastyle:off println
package org.apache.spark.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.util.IntParam

/**
 * Receives text from multiple rawNetworkStreams and counts how many '\n' delimited
 * lines have the word 'the' in them. This is useful for benchmarking purposes. This
 * will only work with spark.streaming.util.RawTextSender running on all worker nodes
 * and with Spark using Kryo serialization (set Java property "spark.serializer" to
 * "org.apache.spark.serializer.KryoSerializer").
 * Usage: RawNetworkGrep <numStreams> <host> <port> <batchMillis>
 * <numStream> is the number rawNetworkStreams, which should be same as number
 * of work nodes in the cluster
 * <host> is "localhost".
 * <port> is the port on which RawTextSender is running in the worker nodes.
 * <batchMillise> is the Spark Streaming batch duration in milliseconds.
 */
object RawNetworkGrep {
	def main(args: Array[String]) {
		if (args.length != 4) {
			System.err.println("Usage: RawNetworkGrep <numStreams> <host> <port> <batchMillis>")
			System.exit(1)
		}
		
		StreamingExamples.setStreamingLogLevels()
		
		val Array(IntParam(numStreams), host, IntParam(port), IntParam(batchMillis)) = args
		val sparkConf = new SparkConf().setAppName("RawNetworkGrep")
		// Create the context
		val ssc = new StreamingContext(sparkConf, Duration(batchMillis))
		
		val rawStreams = (1 to numStreams).map(_ =>
			ssc.rawSocketStream[String](host, port, StorageLevel.MEMORY_ONLY_SER_2)).toArray
		val union = ssc.union(rawStreams)
		union.filter(_.contains("the")).count().foreachRDD(r =>
			println("Grep count: " + r.collect().mkString))
		ssc.start()
		ssc.awaitTermination()
	}
}

// scalastyle:on println
