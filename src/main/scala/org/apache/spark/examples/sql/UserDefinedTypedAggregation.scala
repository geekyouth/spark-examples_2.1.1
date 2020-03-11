package org.apache.spark.examples.sql

// $example on:typed_custom_aggregation$

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
// $example off:typed_custom_aggregation$

object UserDefinedTypedAggregation {
	
	// $example on:typed_custom_aggregation$
	case class Employee(name: String, salary: Long)
	
	case class Average(var sum: Long, var count: Long)
	
	object MyAverage extends Aggregator[Employee, Average, Double] {
		// A zero value for this aggregation. Should satisfy the property that any b + zero = b
		def zero: Average = Average(0L, 0L)
		
		// Combine two values to produce a new value. For performance, the function may modify `buffer`
		// and return it instead of constructing a new object
		def reduce(buffer: Average, employee: Employee): Average = {
			buffer.sum += employee.salary
			buffer.count += 1
			buffer
		}
		
		// Merge two intermediate values
		def merge(b1: Average, b2: Average): Average = {
			b1.sum += b2.sum
			b1.count += b2.count
			b1
		}
		
		// Transform the output of the reduction
		def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count
		
		// Specifies the Encoder for the intermediate value type
		def bufferEncoder: Encoder[Average] = Encoders.product
		
		// Specifies the Encoder for the final output value type
		def outputEncoder: Encoder[Double] = Encoders.scalaDouble
	}
	
	// $example off:typed_custom_aggregation$
	
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
			.builder()
			.appName("Spark SQL user-defined Datasets aggregation example")
			.getOrCreate()
		
		import spark.implicits._
		
		// $example on:typed_custom_aggregation$
		val ds = spark.read.json("examples/src/main/resources/employees.json").as[Employee]
		ds.show()
		// +-------+------+
		// |   name|salary|
		// +-------+------+
		// |Michael|  3000|
		// |   Andy|  4500|
		// | Justin|  3500|
		// |  Berta|  4000|
		// +-------+------+
		
		// Convert the function to a `TypedColumn` and give it a name
		val averageSalary = MyAverage.toColumn.name("average_salary")
		val result = ds.select(averageSalary)
		result.show()
		// +--------------+
		// |average_salary|
		// +--------------+
		// |        3750.0|
		// +--------------+
		// $example off:typed_custom_aggregation$
		
		spark.stop()
	}
	
}
