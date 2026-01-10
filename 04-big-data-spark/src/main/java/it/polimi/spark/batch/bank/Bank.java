package it.polimi.spark.batch.bank;


import static org.apache.spark.sql.functions.max;

import java.util.ArrayList;
import java.util.List;

import it.polimi.spark.common.Consts;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

/**
 * Bank example
 *
 * Input: csv files with list of deposits and withdrawals, having the following
 * schema ("person: String, account: String, amount: Int)
 *
 * Queries
 * Q1. Print the total amount of withdrawals for each person.
 * Q2. Print the person with the maximum total amount of withdrawals
 * Q3. Print all the accounts with a negative balance
 */
public class Bank {
	private static final boolean useCache = true;

	public static void main(String[] args) throws Exception {
		final String master = args.length > 0 ? args[0] : Consts.MASTER_ADDR_DEFAULT;
		final String filePath = args.length > 1 ? args[1] : Consts.FILE_PATH_DEFAULT;
		final String appName = useCache ? "BankWithCache" : "BankNoCache";

		final SparkSession spark = SparkSession
				.builder()
				.master(master)
				.appName(appName)
				.config("spark.eventLog.enabled", "true")
			    .config("spark.eventLog.dir", "file:///tmp/spark-events")
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
		final long maxTotal = sumWithdrawals
				.agg(max("sum(amount)")) // max("sum(amount)") means: "Find the maximum value in column sum(amount)".
				.first() // Extracts the first row from that Dataset (the one that contains only max(sumAMount).
				.getLong(0);

		// Show the row corresponding to the person with maximum total amount of withdrawals.
		final Dataset<Row> maxWithdrawals = sumWithdrawals
				.filter(sumWithdrawals.col("sum(amount)").equalTo(maxTotal));

		maxWithdrawals.show();

		// Show the person with  maximum total amount of withdrawals
		final Dataset<Row> person_maxWithdrawals = sumWithdrawals
				.select("person")
				.where(sumWithdrawals.col("sum(amount)").equalTo((maxTotal)));

		person_maxWithdrawals.show();


		// Q3 Accounts with negative balance

		System.out.println("totWithdrawals:\n");
		final Dataset<Row> totWithdrawals = withdrawals
				.groupBy("account")
				.sum("amount")
				.drop("person")
				.withColumnRenamed("sum(amount)", "totalWithdrawals");

		totWithdrawals.show();

		System.out.println("totDeposits:\n");
		final Dataset<Row> totDeposits = deposits
				.groupBy("account")
				.sum("amount")
				.drop("person")
				.withColumnRenamed("sum(amount)", "totalDeposits");

		totDeposits.show();


		// LEFT JOIN: keep all accounts even if they appear only in withdrawals
		System.out.println("join:\n");
		final Dataset<Row> joinAccounts = totWithdrawals
				.join(totDeposits, "account", "full_outer");  // Spark automatically joins on the "account" column
			  //.na().fill(0, new String[]{"totalWithdrawals", "totalDeposits"}); //Replace nulls with 0: You can avoid the null check in negativeBalances datasets

		joinAccounts.show();
		
		// Filter condition: withdrawals > deposits OR deposits is null
		System.out.println("negativeAccounts:\n");
		final Dataset<Row> negativeBalances = joinAccounts
		        .filter(
		        		joinAccounts.col("totalDeposits").isNull()           // no deposits
		                        .and(joinAccounts.col("totalWithdrawals").gt(0)) // but withdrawals exist
		                .or(
		                		joinAccounts.col("totalWithdrawals").gt(joinAccounts.col("totalDeposits")) // withdrawals > deposits
		                )
		        )
		        .select("account"); // only return the account

		negativeBalances.show();
		
		// Q4 Print all accounts in descending order of balance.
		System.out.println("account | withdrawals and deposits ");
		final Dataset<Row> sumWithdrawalsQ4 = withdrawals
				.groupBy("account")
				.sum("amount")
				.withColumnRenamed("sum(amount)", "totalWithdrawals");
				//.drop("person"); intuitive but not necessary: All non-grouped, non-aggregated columns (like person) are dropped automatically.
		sumWithdrawalsQ4.show();
		
		final Dataset<Row> sumDepositsQ4 = deposits
				.groupBy("account")
				.sum("amount")
				.withColumnRenamed("sum(amount)", "totalDeposits");
				//.drop("person"); intuitive but not necessary: All non-grouped, non-aggregated columns (like person) are dropped automatically.
		sumDepositsQ4.show();
		
		System.out.println("join:\n");
		final Dataset<Row> joinAccountsQ4 = sumWithdrawalsQ4
				.join(sumDepositsQ4, "account", "full_outer")
				.na().fill(0, new String[]{"totalWithdrawals", "totalDeposits"});
		joinAccountsQ4.show();
		
		// Calculate balance and sort
		final Dataset<Row> finalSortedBalanceQ4 = joinAccountsQ4
		    .withColumn("balance", col("totalDeposits").minus(col("totalWithdrawals")))
		    .sort(desc("balance"));

		System.out.println("\n--- Final Account Balances (Ascending Order) ---");
		finalSortedBalanceQ4.show(); 

		spark.close();
	}
}