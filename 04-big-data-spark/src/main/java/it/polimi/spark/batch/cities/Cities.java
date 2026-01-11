package it.polimi.spark.batch.cities;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import it.polimi.spark.common.Consts;
import scala.Tuple3;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;


public class Cities {
	private static final boolean useCache = true;

	public static void main(String[] args) throws TimeoutException, IOException {
		final String master = args.length > 0 ? args[0] : Consts.MASTER_ADDR_DEFAULT;
		final String filePath = args.length > 1 ? args[1] : Consts.FILE_PATH_DEFAULT;
		final String appName = useCache ? "CityWithCache" : "CityNoCache";

		final SparkSession spark = SparkSession
				.builder()
				.master(master)
				.appName(appName)
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");


		// cityRegion Dataset
		final List<StructField> citiesRegionSchemaFields = new ArrayList<>();
		citiesRegionSchemaFields.add(DataTypes.createStructField("city", DataTypes.StringType, false));
		citiesRegionSchemaFields.add(DataTypes.createStructField("region", DataTypes.StringType, false));
		final StructType citiesRegionSchema = DataTypes.createStructType(citiesRegionSchemaFields);

		final Dataset<Row> citiesRegion = spark
				.read()
				.option("header", "true")
				.option("delimiter", ";")
				.schema(citiesRegionSchema)
				.csv(filePath + "files/cities/cities_regions.csv");

		// citiesPopulation Dataset
		final List<StructField> citiesPopulationSchemaFields = new ArrayList<>();
		citiesPopulationSchemaFields.add(DataTypes.createStructField("id", DataTypes.StringType, false));
		citiesPopulationSchemaFields.add(DataTypes.createStructField("city", DataTypes.StringType, false));
		citiesPopulationSchemaFields.add(DataTypes.createStructField("population", DataTypes.IntegerType, false));
		final StructType citiesPopulationSchema = DataTypes.createStructType(citiesPopulationSchemaFields);

		final Dataset<Row> citiesPopulation = spark
				.read()
				.option("header", "true")
				.option("delimiter", ";")
				.schema(citiesPopulationSchema)
				.csv(filePath + "files/cities/cities_population.csv");	

		if (useCache) {
			citiesPopulation.cache();
		}

		// Q1. Compute the total population for each region
		final Dataset<Row> citiesRegionPopulation  = citiesRegion
				.join(citiesPopulation, "city", "full_outer"); 
		
		//ALTERNATIVE: 
		/*.join(citiesPopulation, citiesRegion.col("city").equalTo(citiesPopulation.col("city"))); 
        .select(citiesPopulation.col("id"),
                citiesPopulation.col("city"),
                citiesRegions.col("region"),
                citiesPopulation.col("population"));
        */


		if (useCache) {
			citiesRegionPopulation.cache();
		}

		final Dataset<Row> totalPopulation = citiesRegionPopulation
				.groupBy("region")
				.sum("population")
				.sort(desc("sum(population)"));
		// select("region", "sum(population)"); NOT NECESSARY

		System.out.println("QUERY 1: Compute the total population for each region");
		totalPopulation.show();


		// Q2: compute the number of cities and the population of the most populated city for each region
		final Dataset<Row> mostPopulatedCity = citiesRegionPopulation
				.groupBy("region")
				.agg(count("city"), max("population"))
				.sort(desc("max(population)"));
		System.out.println("QUERY 2: compute the number of cities and the population of the most populated city for each region");
		mostPopulatedCity.show();


		// Q3: 
		/*
		 * Print the evolution of the population in Italy year by year until the total population in Italy overcomes 100M people
			Assume that the population evolves as follows:
					•In cities with more than 1000 inhabitants, it increases by 1% every year
					•In cities with less than 1000 inhabitants, it decreased by 1% every year
			The output on the terminal should be a sequence of lines
				•Year: 1, total population: xxx
				•Year: 2, total population: yyy
			•You may round the population of each city to the nearest integer during computation
		 */



		JavaRDD<Integer> populationProgress = citiesPopulation.toJavaRDD().map(r -> r.getInt(2));
		populationProgress.cache();
		long count  = populationProgress.reduce((a, b) -> a+b);


		int year = 0;
		while (count < 100 * 1000 * 1000) {
			year++;
			populationProgress = populationProgress.map(p -> p > 1000 ? p+(int)(p*0.01) : p-(int)(p*0.01));
			populationProgress.cache();
			count = populationProgress.reduce((a, b) -> a+b);
			System.out.println("Year: " + year + ", count: " + count);
		}


		// Q4: compute the total number of bookings for each region, in a window of 30 seconds, sliding every 5 seconds

		final Dataset<Row> bookings = spark
				.readStream()
				.format("rate")
				.option("rowsPerSecond", 100)
				.load();

		//bookings = bookings.withColumn("value", col("value").mod(3374)); // Just to have more meaningful log, (not strictly necessary)

		final StreamingQuery q4 = bookings
				.join(
						citiesRegionPopulation,
						bookings.col("value").equalTo(citiesRegionPopulation.col("id")))
				.drop("population", "value")
				.groupBy(
						window(col("timestamp"), "30 seconds", "5 seconds"),
						col("region")
						)
				.count()
				.writeStream()
				.outputMode("update")
				.format("console")
				.start();


		try {
			q4.awaitTermination();
		} catch (final StreamingQueryException e) {
			e.printStackTrace();
		}


		spark.close();

	}
}