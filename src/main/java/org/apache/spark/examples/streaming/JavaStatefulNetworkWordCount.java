package org.apache.spark.examples.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Counts words cumulatively in UTF8 encoded, '\n' delimited text received from the network every
 * second starting with initial value of word count.
 * Usage: JavaStatefulNetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive
 * data.
 * <p>
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 * and then run the example
 * `$ bin/run-example
 * org.apache.spark.examples.streaming.JavaStatefulNetworkWordCount localhost 9999`
 */
public class JavaStatefulNetworkWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");
	
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: JavaStatefulNetworkWordCount <hostname> <port>");
			System.exit(1);
		}
		
		StreamingExamples.setStreamingLogLevels();
		
		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("JavaStatefulNetworkWordCount");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
		ssc.checkpoint(".");
		
		// Initial state RDD input to mapWithState
		@SuppressWarnings("unchecked")
		List<Tuple2<String, Integer>> tuples =
			Arrays.asList(new Tuple2<>("hello", 1), new Tuple2<>("world", 1));
		JavaPairRDD<String, Integer> initialRDD = ssc.sparkContext().parallelizePairs(tuples);
		
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
			args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER_2);
		
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String x) {
				return Arrays.asList(SPACE.split(x)).iterator();
			}
		});
		
		JavaPairDStream<String, Integer> wordsDstream = words.mapToPair(
			new PairFunction<String, String, Integer>() {
				@Override
				public Tuple2<String, Integer> call(String s) {
					return new Tuple2<>(s, 1);
				}
			});
		
		// Update the cumulative count function
		Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc =
			new Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>() {
				@Override
				public Tuple2<String, Integer> call(String word, Optional<Integer> one,
													State<Integer> state) {
					int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
					Tuple2<String, Integer> output = new Tuple2<>(word, sum);
					state.update(sum);
					return output;
				}
			};
		
		// DStream made of get cumulative counts that get updated in every batch
		JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream =
			wordsDstream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD));
		
		stateDstream.print();
		ssc.start();
		ssc.awaitTermination();
	}
}
