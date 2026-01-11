package it.polimi.spark.batch.iterative;

import it.polimi.spark.common.Consts;
import scala.Tuple2;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Start from a dataset of investments. Each element is a Tuple2(amount_owned, interest_rate).
 * At each iteration the new amount is (amount_owned * (1+interest_rate)).
 *
 * Implement an iterative algorithm that computes the new amount for each investment and stops
 * when the overall amount overcomes 1000.
 */
public class InvestmentSimulator {
    public static void main(String[] args) {
        final String master = args.length > 0 ? args[0] : Consts.MASTER_ADDR_DEFAULT;
        final String filePath = args.length > 1 ? args[1] : Consts.FILE_PATH_DEFAULT;
        final double threshold = 30;

        final SparkConf conf = new SparkConf().setMaster(master).setAppName("InvestmentSimulator");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        final JavaRDD<String> textFile = sc.textFile(filePath + "files/iterative/investment.txt");
        
        // Transform each line into a tuple (amount:owned, investment_rate)
        JavaRDD<Tuple2<Double, Double>> investments = textFile.map(s -> {
        	String[] values = s.split(" ");
        	double amountOwned = Double.parseDouble(values[0]);
        	double investementRate = Double.parseDouble(values[1]);
        	return new Tuple2<>(amountOwned, investementRate);
        });
        
        int iteration = 0;
        double sum = sumAmount(investments);
        /*
         * Spark does not have any native iterative operator 
         * Notice that you cannot iterate over spark jobs.
         * You need to iterate inside the driver program. (java programming)
         */
        while(sum < threshold) {
        	iteration++;
        	System.out.println("Iteration " +iteration); 
        	/*
        	 * 
        	 */
        	investments = investments.map(i -> {
        		/* 
        		 * So we print each time we execute map(). 
        		 * So for each job (for each iteration) we expect to execute the print 5 times since we have 5 lines in investment.txt
        		 */
        		System.out.println("AAA"); 
        		return new Tuple2<>(i._1*(1+i._2), i._2);	
        		
        	});
        	/*
        	 * In the first iteration the following sumAmount() will need only one pass over investments.
        	 * In the second iteration the investments need to start from the result of the previous iteration;
        	 * 		However those results were computed in a different job and we didn't save them in memory:
        	 * 			So Spark in the second iteration execute map twice for each investments.
        	 * And so on.
        	 * 
        	 * So the problem is that job results are based on previous job results but job does not share results.
        	 * If we want to persist the result of a job we can cache them with the cache() method.
        	 * 
        	 * 
        	 */
        	investments.cache();
        	sum = sumAmount(investments); //starts a new job at each iteration.
       
        }

        System.out.println("Sum: " + sum + " after " + iteration + " iterations");
        sc.close();
    }

	private static final double sumAmount(JavaRDD<Tuple2<Double, Double>> investments) {
		return investments
				.mapToDouble(a -> a._1)
				.sum(); // sum() is an action --> it starts a job onto spark. So the driver (java) program waits for the job to complete and when it completes
		 										 // it gets the result
	}

}
