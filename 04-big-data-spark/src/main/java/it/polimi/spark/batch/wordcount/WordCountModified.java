package it.polimi.spark.batch.wordcount;

import it.polimi.spark.common.Consts;
import scala.Tuple2;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordCountModified {

	public static void main(String[] args) {
		final String master = args.length > 0 ? args[0] : Consts.MASTER_ADDR_DEFAULT;
		final String filePath = args.length > 1 ? args[1] : Consts.FILE_PATH_DEFAULT;

		final SparkConf conf = new SparkConf().setMaster(master).setAppName("WordCount");
		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");


		// Q1. For each character, compute the number of words starting with that character
		final JavaRDD<String> lines = sc.textFile(filePath + "files/wordcount/in.txt");
		final JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator()); // each line is transformed into multiple words
		final JavaPairRDD<Character, Integer> initialWordCharPairs = words.mapToPair(s -> new Tuple2<>(s.charAt(0), 1)); //map each word into a tuple <intialLetterOfThatWord, count> with count initialized at 1
		final JavaPairRDD<Character, Integer> countsQ1 = initialWordCharPairs.reduceByKey((a, b) -> a + b); //given two values for the same key we reduce it to the sum
		System.out.println("Results of query Q1");
		System.out.println(countsQ1.collect()); // collect the result of this driver program


		// Q2. For each character, compute the number of lines starting with that character
		final JavaPairRDD<Character, Integer> initialLineCharPairs = lines.mapToPair(s -> new Tuple2<>(s.charAt(0),1)); //map each line into a tuple <intialLetterOfThatLine, count> with count initialized at 1
		final JavaPairRDD<Character, Integer> countsQ2 = initialLineCharPairs.reduceByKey((a, b) -> a+b); //given two values for the same key we reduce it to the sum
		System.out.println("Results of query Q2");
		System.out.println(countsQ2.collect()); // collect the result of this driver program

		// Q3. Compute the average number of characters in each line
		Tuple2<Integer, Integer> q3 = lines
				.mapToPair(s -> new Tuple2<>(s.length(), 1)) //each line is transformed into a tuple <lineLength, 1>
				.reduce((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2)); // reduced to <sumOfLength, nOfLine>
		System.out.println("Results of query Q3");
		System.out.println(((float) q3._1) / q3._2);

		// Alternative way for Q3.
		final long nOfLines = lines.count();
		final JavaRDD<Integer> nOfCharacters = lines.map(line -> line.length()); // each line is transformed into its length in terms of characters
		final double totalCharacters = nOfCharacters.mapToDouble(x -> x).sum();  // sum all lengths
		System.out.println("Alternative way for query Q3");
		System.out.println(totalCharacters / nOfLines);

		sc.close();
	}

}