package org.apache.spark.examples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * Computes an approximation to pi
 * Usage: JavaSparkPi [slices]
 */
public final class JavaSparkPi {
	
	public static void main(String[] args) throws Exception {
		SparkSession spark = SparkSession
			.builder()
			.appName("JavaSparkPi")
			.getOrCreate();
		
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		
		int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
		int n = 100000 * slices;
		List<Integer> l = new ArrayList<>(n);
		for (int i = 0; i < n; i++) {
			l.add(i);
		}
		
		JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);
		
		int count = dataSet.map(new Function<Integer, Integer>() {
			@Override
			public Integer call(Integer integer) {
				double x = Math.random() * 2 - 1;
				double y = Math.random() * 2 - 1;
				return (x * x + y * y < 1) ? 1 : 0;
			}
		}).reduce(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer integer, Integer integer2) {
				return integer + integer2;
			}
		});
		
		System.out.println("Pi is roughly " + 4.0 * count / n);
		
		spark.stop();
	}
}
