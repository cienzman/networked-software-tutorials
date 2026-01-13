package it.polimi.spark.streaming.enrichment;

import it.polimi.spark.common.Consts;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.window;

import java.util.ArrayList;
import java.util.List;

public class EventEnrichment_RateSource {
    public static void main(String[] args) throws Exception {
        final String master = args.length > 0 ? args[0] : Consts.MASTER_ADDR_DEFAULT;
        final String filePath = args.length > 1 ? args[1] : Consts.FILE_PATH_DEFAULT; //"./";
        
        // Note: socketHost and socketPort are unused since we use the 'rate' source.
        /*  
        final String socketHost = args.length > 2 ? args[2] : Consts.SOCKET_HOST_DEFAULT;
        final int socketPort = args.length > 3 ? Integer.parseInt(args[3]) : Consts.SOCKET_PORT_DEFAULT;
         */
        
        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName("EventEnrichment")
                .getOrCreate();

        final List<StructField> productClassificationFields = new ArrayList<>();
        productClassificationFields.add(DataTypes.createStructField("product", DataTypes.StringType, false));
        productClassificationFields.add(DataTypes.createStructField("classification", DataTypes.StringType, false));
        final StructType productClassificationSchema = DataTypes.createStructType(productClassificationFields);

        // Create DataFrame representing the stream of input products from connection to localhost:9999
        		// - timestamp: The time when the record was generated (a TimestampType).
		 		// - value: A continuously increasing counter (a LongType) at a fixed rate.
        final Dataset<Row> inStream = spark
				.readStream()
				.format("rate")
				.option("rowsPerSecond", 1)
				.load();
		spark.sparkContext().setLogLevel("ERROR");

        //Dataset<Row> inStreamDF = inStream.toDF("timestamp", "product"); // rename the default "value" column to "product" for the sake of join operation.
		Dataset<Row> inStreamDF = inStream.withColumn("value", col("value").mod(10)); // restricts the value to a range of 0 to 9 to make the join work in a more meaningful way.
        

        final Dataset<Row> productsClassification = spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(productClassificationSchema)
                .csv(filePath + "files/enrichment/product_classification.csv");

        // Query: count the number of products of each class in the stream
        
        final StreamingQuery query = inStreamDF  
                .join(productsClassification, inStreamDF.col("value").equalTo(productsClassification.col("product")))
                .groupBy(window(col("timestamp"), "30 seconds", "5 seconds"), 
						col("classification"))
                .count()
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        try {
            query.awaitTermination();
        } catch (final StreamingQueryException e) {
            e.printStackTrace();
        }

        spark.close();
    }
}