# Big Data Analytics with Apache Spark

## Overview
This module explores **Big Data Analytics** using **Apache Spark**. It covers the entire spectrum of Spark programming models, from the low-level **RDD (Resilient Distributed Dataset)** API to the high-level **Structured APIs (Datasets/DataFrames)** and **Structured Streaming**.

Key concepts demonstrated include:
* **Distributed Processing:** Transformations (map, flatMap, filter) and Actions (reduce, count, collect).
* **Structured Queries:** Using SQL-like operators on distributed data.
* **Iterative Algorithms:** Managing caching and persistence for performance optimization in loops.
* **Streaming:** Processing data streams with windowing operations.



## Prerequisites & Setup

To experiment with these examples, you need to set up Apache Spark on your machine.

### 1. Install Java & Spark
1.  **Java Requirement:** Ensure you are using **Java 17** or **Java 21**.
    * *Note:* Later versions or older versions may not work properly.
2.  **Download Spark:** Get **Spark 4.0.1** from the [Official Spark Downloads](https://spark.apache.org/downloads.html).
3.  **Setup:**
    * Unzip the archive.
    * For a distributed deployment, you can run the components on your machine (Standalone Mode) or use Docker containers.

### 2. Troubleshooting (Java >= 9)
If Spark fails to access internal Java modules (common with newer Java versions), you must add the following JVM options when running your application:

```bash
--add-opens java.base/java.nio=ALL-UNNAMED \
--add-opens java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens java.base/java.util=ALL-UNNAMED
```
### 3. Project Configuration
* **Dependencies:** Ensure your `pom.xml` (or build tool) includes the `spark-core` and `spark-sql` libraries matching version 4.0.1.
* **Constants:** The file `Consts.java` defines the default **Master Address** and **File Paths**. Modify this file to match your local setup (e.g., changing `spark://...` to `local[*]`).

---

## Code Structure & Concepts

The examples are categorized by the Spark abstractions they utilize.

### 1. RDD API (The Basics)
These examples use the fundamental RDD abstraction for unstructured data processing.
* **`WordCount.java`**: The "Hello World" of Big Data. It reads a text file, splits lines into words (`flatMap`), maps them to pairs (`mapToPair`), and aggregates counts (`reduceByKey`). It demonstrates the basic MapReduce pattern in Spark.

### 2. Structured API (Batch Processing)
These examples use `Dataset<Row>` (DataFrames) for querying structured data (CSV, JSON) using a schema-aware approach.
* **`Bank.java`**: Performs analytics on financial data (csv schema: `person`, `account`, `amount`). It runs complex queries:
    * Calculating total withdrawals per person (`groupBy`, `sum`).
    * Identifying accounts with negative balances using Joins.
    * Sorting accounts by final balance.

### 3. Iterative Algorithms & Caching
Spark is optimized for iterative algorithms where the same dataset is reused multiple times. These examples demonstrate **State Management** and **Memory Persistence**.
* **`InvestmentSimulator.java`**: Simulates compound interest growth in a loop. It demonstrates the critical use of `.cache()` to persist intermediate results in memory across iterations, avoiding re-computation of the entire lineage.
* **`InvestmentSimulatorUnpersist.java`**: An advanced version that manually manages memory using `.unpersist()`. It frees up memory from old iterations that are no longer needed, optimizing resource usage on the executors.
* **`TransitiveClosureFriends.java`**: Implements a graph algorithm to find "friends of friends" (transitive closure). It repeatedly joins a dataset with itself until convergence, highlighting the trade-off between join costs and table sizes.

### 4. Hybrid & Streaming Processing
* **`Cities.java`**: A comprehensive example combining batch and streaming:
    * **Batch:** Simulates population growth over years using RDD transformations (`map`, `reduce`).
    * **Streaming (Q4):** Uses **Structured Streaming** to join a generated rate stream with static city data. It computes the number of bookings per region in a **sliding window** (30-second window, sliding every 5 seconds).

---

## How to Run

You can run these applications directly from your IDE or submit them to a Spark Cluster.

**Run from IDE (Local Mode):**
1.  Open `Consts.java` and set `MASTER_ADDR_DEFAULT` to `"local[4]"` (to use 4 local cores).
2.  Run the `main` method of the desired class (e.g., `WordCount`).

**Run on Cluster usefull commands**
```bash
./sbin/start-master.sh  
./sbin/stop-master.sh  
./sbin/start-worker.sh spark://local-host:7077 
./sbin/stop-worker.sh spark://local-host:7077 
./sbin/start-history-server.sh
./sbin/stop-history-server.sh  
```
In order to visualize the Master UI:
```bash
http://localhost:8080/ 
```


