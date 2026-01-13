package it.polimi.spark.streaming.event_time;

import it.polimi.spark.common.Consts;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static org.apache.spark.sql.functions.*;

/*
 * Input data can represent events that occur at some point in time. (e.g, informations that come from sensors).
 * This time is indicated ad timestamp within the input: the sensor can attach a timestamp that indicates when the reading was performed
 * from the sensor point of view.
 * 
 */
public class WindowedCount {
	public static void main(String[] args) throws Exception {
		final String master = args.length > 0 ? args[0] : Consts.MASTER_ADDR_DEFAULT;

		final SparkSession spark = SparkSession
				.builder()
				.master(master)
				.appName("WindowedCount")
				.getOrCreate();

		/*
		 * .readStream().format("rate"): This defines a stream DataFrame using the built-in rate source.
		 * This source is used for testing and benchmarking and continuously generates two columns:
		 * 		- timestamp: The time when the record was generated (a TimestampType).
		 * 		- value: A continuously increasing counter (a LongType) at a fixdd rate.
		 * 		- option("rowsPerSecond", 1): Configures the stream to inject one new record every second.
		 */
		final Dataset<Row> inputRecords = spark
				.readStream()
				.format("rate")
				.option("rowsPerSecond", 1)
				.load();
		spark.sparkContext().setLogLevel("ERROR");

		/*
		 * It tells Spark that data arriving up to 1 hour late (based on its timestamp column) should still be processed.
		 * More importantly, it allows Spark to safely discard the state for old windows:
		 * 		 Once Spark determines that it won't receive any more records for a window
		 * 		 (because the current time is more than 1 hour past the end of that window),
		 * 		 it will cleanup the state for that window, preventing memory from growing indefinitely.
		 */
		inputRecords.withWatermark("timestamp", "1 hour"); 


		final StreamingQuery query = inputRecords
				.withColumn("value", col("value").mod(5)) // restricts the value to a range of 0 to 4 making the counting results more interesting than having only increasing value that never repets
				.withColumn("timestamp", date_format(col("timestamp"), "HH:mm:ss"))
				.groupBy(window(col("timestamp"), "30 seconds", "10 seconds"), // see below for in depth explanation about groupBy
						col("value")
						)
				.count()
				.writeStream()
				.outputMode("update")
				.format("console")
				// .option("truncate", false) // Useful to see the full window struct if you don't drop it
				.start();
		
        try {
            query.awaitTermination(); // Blocks the application and keeps it running until the query is manually stopped (e.g., by stopping the JVM).
        } catch (final StreamingQueryException e) {
            e.printStackTrace();
        }

		spark.close();
	}
}


/*
 * GROUPING STRATEGIES:
 *  1: Group by window and by value: "How many times did Value X appear during this specific time period?"
 *  	AI generated example: Use Case: Trending topics. You don't just want to know that "Justin Bieber" was mentioned 1 million times total; you want to know he was mentioned 50 times between 12:00 and 12:30.
 *  
 *  2: Group by window: "How many events happened total during this time period?"
 *  3: Group by value: "How many times has Value X appeared since the beginning of time?"
 */





















