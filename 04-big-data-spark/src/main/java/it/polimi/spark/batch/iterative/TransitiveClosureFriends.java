package it.polimi.spark.batch.iterative;

import it.polimi.spark.common.Consts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;


/**
 * Implement an iterative algorithm that implements the transitive closure of friends
 * (people that are friends of friends of ... of my friends).
 * <p>
 * Set the value of the flag useCache to see the effects of caching.
 */
public class TransitiveClosureFriends {
    private static final boolean useCache = false;


    public static void main(String[] args) throws IOException {
        final String master = args.length > 0 ? args[0] : Consts.MASTER_ADDR_DEFAULT;
        final String filePath = args.length > 1 ? args[1] : Consts.FILE_PATH_DEFAULT;
        final String appName = useCache ? "FriendsCache" : "FriendsNoCache";


        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");


        final List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("person", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("friend", DataTypes.StringType, false));
        final StructType schema = DataTypes.createStructType(fields);


        final Dataset<Row> input = spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(schema)
                .csv(filePath + "files/iterative/friends.csv");


        if (useCache) {
            input.cache();
        }


        Dataset<Row> allFriends = input;
        long oldCount = 0;
        long newCount = allFriends.count();
        int iteration = 0;


        while (newCount > oldCount) {
            iteration++;
            // One could also join allFriends with allFriends.
            // It is a tradeoff between the number of joins and the size of the tables to join.
            Dataset<Row> joinFriends =
                    allFriends.as("all-friends")
                            .join(
                                    input.as("input"),
                                    col("all-friends.friend").equalTo(col("input.person"))
                            );
            System.out.println("\n Join Friends \n");
            joinFriends.show();
            
            Dataset<Row> newFriends =
            		joinFriends.select(
                                    col("all-friends.person").as("person"),
                                    col("input.friend").as("friend")
                            );
            
            allFriends = allFriends
                    .union(newFriends)
                    .distinct();


            if (useCache) {
                allFriends.cache();
            }
            oldCount = newCount;
            newCount = allFriends.count();
            System.out.println("Iteration: " + iteration + " - Count: " + newCount);
        }
        
        System.out.println("\n Final Transitive Closure \n");
        allFriends.show();
        spark.close();
    }
}

















