package it.polimi.spark.batch.wordcount;

import java.util.Arrays;

import it.polimi.spark.common.Consts;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {

    public static void main(String[] args) {
        final String master = args.length > 0 ? args[0] : Consts.MASTER_ADDR_DEFAULT; //"local[4]";
        final String filePath = args.length > 1 ? args[1] : Consts.FILE_PATH_DEFAULT; // "./";

        final SparkConf conf = new SparkConf().setMaster(master).setAppName("WordCount");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<String> lines = sc.textFile(filePath + "files/wordcount/in.txt"); //read lines from text_file
        final JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator()); // each line is transformed into multiple words
        final JavaPairRDD<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1)); //map each word into a tuple <word, count> with count initialized at 1
        final JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b); //given two values for the same key we reduce it to the sum
        System.out.println(counts.collect()); // collect the result of this driver program

        sc.close();
    }

}