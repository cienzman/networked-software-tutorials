package it.polimi.spark.batch.bank;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import static org.apache.spark.sql.functions.*;


/**
 * THIS IS SIMPLY AN ALTERNATIVE VERSION FOR QUERY 3 AND 4
 */
public class BankAlternativeVersion {
    private static final boolean useCache = true;


    public static void main(String[] args) throws IOException {
        final String master = args.length > 0 ? args[0] : "local[4]";
        // final String master = args.length > 0 ? args[0] : "spark://127.0.0.1:7077";
        final String filePath = args.length > 1 ? args[1] : "./";
        // final String filePath = args.length > 1 ? args[1] : "/Users/margara/Desktop/";
        final String appName = useCache ? "BankWithCache" : "BankNoCache";


        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");


        final List<StructField> mySchemaFields = new ArrayList<>();
        mySchemaFields.add(DataTypes.createStructField("person", DataTypes.StringType, true));
        mySchemaFields.add(DataTypes.createStructField("account", DataTypes.StringType, true));
        mySchemaFields.add(DataTypes.createStructField("amount", DataTypes.IntegerType, true));
        final StructType mySchema = DataTypes.createStructType(mySchemaFields);


        final Dataset<Row> deposits = spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(mySchema)
                .csv(filePath + "files/bank/deposits.csv");


        final Dataset<Row> withdrawals = spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(mySchema)
                .csv(filePath + "files/bank/withdrawals.csv");


        // Used in two different queries
        if (useCache) {
            withdrawals.cache();
        }


        // Q1. Total amount of withdrawals for each person
        System.out.println("Total amount of withdrawals for each person");


        final Dataset<Row> sumWithdrawals = withdrawals
                .groupBy("person")
                .sum("amount")
                .select("person", "sum(amount)");


        // Used in two different queries
        if (useCache) {
            sumWithdrawals.cache();
        }


        sumWithdrawals.show();


        // Q2. Person with the maximum total amount of withdrawals
        System.out.println("Person with the maximum total amount of withdrawals");


        final long maxTotal = sumWithdrawals
                .agg(max("sum(amount)"))
                .first()
                .getLong(0);


        final Dataset<Row> maxWithdrawals = sumWithdrawals
                .filter(sumWithdrawals.col("sum(amount)").equalTo(maxTotal));


        maxWithdrawals.show();


        // Q3 Accounts with negative balance
        System.out.println("Accounts with negative balance");


        final Dataset<Row> totWithdrawals = withdrawals
                .groupBy("account")
                .sum("amount")
                .drop("person")
                .as("totalWithdrawals");


        final Dataset<Row> totDeposits = deposits
                .groupBy("account")
                .sum("amount")
                .drop("person")
                .as("totalDeposits");


        final Dataset<Row> negativeAccounts = totWithdrawals
                .join(totDeposits, totDeposits.col("account").equalTo(totWithdrawals.col("account")), "left_outer")
                .filter(totDeposits.col("sum(amount)").isNull().and(totWithdrawals.col("sum(amount)").gt(0)).or
                                (totWithdrawals.col("sum(amount)").gt(totDeposits.col("sum(amount)")))
                ).select(totWithdrawals.col("account"));


        negativeAccounts.show();


        // Q3 Alternative approach
        System.out.println("Accounts with negative balance (second approach)");

        System.out.println("Total Ops");
        Dataset<Row> totalOps = withdrawals
                .withColumn("amount", col("amount").multiply(-1))
                .union(deposits);
        totalOps.show();


        Dataset<Row> accountBalances = totalOps
                .groupBy("account")
                .sum(); // All non-grouped, non-aggregated columns (like person) are dropped automatically.
        accountBalances.show();

        /*
         * - cache() stores the Dataset in memory the first time it is computed.
         * - Later actions (like .show(), .sort(), .filter()) use the cached data without recomputing the entire lineage graph.
         * - Spark DataFrames are lazy and are built using a Directed Acyclic Graph (DAG) of transformations. When we call an action (like show()
         * 		 or writeStream()), Spark re-executes all previous transformations leading up to that action.
         * -Without caching accountBalances: 
         * 		-- Q3 Action (negativeAccounts2.show()): Spark would re-read deposits.csv, re-read withdrawals.csv, re-perform the union,
         * 				 re-perform the multiply(-1), and re-calculate the final groupBy/sum.
         * 		-- Q4 Action (sortedAccountsBalances.show()): Spark would re-execute the entire chain of transformations again.
         * If you do NOT call cache() Each time you run an action (like show()), Spark recomputes the entire DAG.
         * This is slow and expensive, especially with large datasets.
         */
        accountBalances.cache();


        Dataset<Row> negativeAccounts2 = accountBalances
                .filter(col("sum(amount)").lt(0));


        negativeAccounts2.show();


        // Q4 Accounts in descending order of balance
        // Easy if we used the second approach for query Q3
        System.out.println("Accounts in descending order of balance");


        Dataset<Row> sortedAccountsBalances = accountBalances
                .sort(desc("sum(amount)"));
        sortedAccountsBalances.show();


        spark.close();
    }
}
