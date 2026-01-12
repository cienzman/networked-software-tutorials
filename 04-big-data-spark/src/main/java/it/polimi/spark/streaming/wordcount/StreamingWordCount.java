package it.polimi.spark.streaming.wordcount;

import java.util.Arrays;

import it.polimi.spark.common.Consts;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

@SuppressWarnings("deprecation")
public class StreamingWordCount {
    @SuppressWarnings("deprecation")
	public static void main(String[] args) {
        final String master = args.length > 0 ? args[0] : Consts.MASTER_ADDR_DEFAULT;
        final String socketHost = args.length > 1 ? args[1] : Consts.SOCKET_HOST_DEFAULT;
        final int socketPort = args.length > 2 ? Integer.parseInt(args[2]) : Consts.SOCKET_PORT_DEFAULT;
        
        // We are going to process a dynamic dataset.
        final SparkConf conf = new SparkConf().setMaster(master).setAppName("StreamingWordCountSum");
        
        /*
         * - This defines the Base Batch Interval.
         * - Divide the continuous stream of incoming data (from the socket) into discrete, time-based collections called micro-batches or RDDs 
         * 		(Resilient Distributed Datasets). 
         * 		In this case, Spark will create a new RDD every 1 second containing all the data that arrived during that second.
         */
        final JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1)); 
        sc.sparkContext().setLogLevel("ERROR");

        final JavaPairDStream<String, Integer> counts = sc.socketTextStream(socketHost, socketPort)
        		/*
        		 * - The window groups the underlying 1-second RDDs together to perform an aggregated operation (like word count)
        		 * 		 over a wider time frame, but only calculates the result periodically.
        		 * - Window Length: At every calculation, Spark looks at the last 10 seconds of data.
        		 * - Sliding Interval: (The frequency). Spark creates a new result every 5 seconds.
        		 * - This transforms the stream into a sliding window -> allows you to group elements together.
        		 * 		windows(size = how much data, slide=how frequently need to be evaluated) 
        		 * - At 00:10: It takes data from 00:00 to 00:10 (full 10s) and counts words.
        		 * - At 00:15: It takes data from 00:05 to 00:15 (dropping the 00:00-00:05 data) and counts words.
        		 */
                .window(Durations.seconds(10), Durations.seconds(5)) 
                .map(String::toLowerCase)
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((a, b) -> a + b);

        counts.foreachRDD(rdd -> rdd //start a new job for each RDD in the stream that is not empty -
                .collect() // This moves the data from the distributed Spark nodes (Executors) back to the main program (Driver).
                .forEach(System.out::println)
        );

        sc.start(); // Nothing actually happens until this line is hit. This tells Spark to begin listening to the socket.

        try {
            sc.awaitTermination(); // Keeps the Java application running continuously until you manually stop it (e.g., Ctrl+C).
        } catch (final InterruptedException e) { 
            e.printStackTrace();
        }
        sc.close();
    }
}