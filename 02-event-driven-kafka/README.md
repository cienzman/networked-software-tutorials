# Event-driven Applications with Apache Kafka

## Overview
This module demonstrates how to build **Event-driven Applications** using **Apache Kafka**. It covers the fundamental building blocks of Kafka programming: Producer/Consumer implementations and advanced concepts like **Exactly-Once Semantics (EOS)**, **Transactions**, and **Idempotence**.


## ⚙️ Prerequisites & Setup

To experiment with these examples, you need to set up your environment and the Kafka broker.

### 1. Install Apache Kafka
1.  **Download:** Get **Kafka 4.1.0** (Binary download for **Scala 2.13**) from the [Official Kafka Downloads](https://kafka.apache.org/downloads).
2.  **Unzip:** Extract the archive to a location of your choice on your machine.
3.  **Start the Server:** Follow the instructions in the Kafka documentation to start the **Zookeeper** instance and the **Kafka Broker**.

### 2. Project Setup
1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/cienzman/networked-software-tutorials.git
    ```
2.  **Import:** Open the project in your favorite IDE (IntelliJ IDEA, Eclipse).
3.  **Compile:** Ensure the project compiles successfully.
    * *Note:* Pay attention to the package names. The code provided is organized into sub-packages (e.g., `it.polimi.middleware.kafka.basic`, `it.polimi.middleware.kafka.transactional`). Ensure your directory structure matches the package declarations.

---

## Code Structure & Concepts

The examples are categorized by the architectural patterns they demonstrate.

### 1. Topic Management
Before running producers or consumers, you often need to manage topics programmatically.
* **`TopicManager.java`**: Uses the `AdminClient` API to check for existing topics, delete them, and create new topics with specific partition counts and replication factors.

### 2. Basic Pub/Sub (At-Least-Once / At-Most-Once)
These files demonstrate the standard Kafka API for sending and receiving messages.
* **`BasicProducer.java`**: Configures a `KafkaProducer` with `StringSerializer`. It demonstrates:
    * **Async Sending:** Using `producer.send(record)`.
    * **Sync Waiting (Ack):** Using `future.get()` to implement a blocking wait for acknowledgments (Stronger durability).
* **`BasicConsumer.java`**: A standard consumer using **Auto-Commit**. It subscribes to a topic and polls for records in a loop. It demonstrates `auto.offset.reset` strategies (`earliest` vs `latest`).
* **`BasicConsumerManual.java`**: Disables auto-commit (`enable.auto.commit = false`) to manually control when offsets are saved. This is critical for ensuring a message is fully processed before it is marked as "read".

### 3. Stream Processing & Transformations
* **`LowerCaseConsumer.java`**: A simple processing pipeline that reads a string, converts it to lowercase, and produces it to a new topic.

### 4. Advanced Reliability (Transactions & EOS)
These examples show how to achieve **Exactly-Once Semantics (EOS)**, ensuring data is processed exactly once even in the event of failures.

* **`IdempotentProducer.java`**: Enables `enable.idempotence = true`. This ensures that if the producer retries sending a message due to a network error, duplicates are not introduced in the log.
* **`TransactionalProducer.java`**: Initiates a transaction (`initTransactions`, `beginTransaction`). It simulates a scenario where some transactions are committed and others are aborted, allowing you to test isolation levels.
* **`TransactionalConsumer.java`**: Configures `isolation.level` to `read_committed`. This ensures the consumer *only* sees messages from successfully committed transactions, ignoring aborted ones.
* **`AtomicForwarder.java`**: The **Read-Process-Write** pattern. It consumes from Topic A and writes to Topic B within a single transaction. Crucially, it sends offsets to the transaction (`producer.sendOffsetsToTransaction`), ensuring that the consumption and production happen atomically.
* **`LowerCaseTransactionalForwarder.java`**: A practical example of Atomic Forwarding that counts occurrences of keys and converts values to lowercase transactionally.

---

## How to Run

Since these are Java applications, you can run the `main` method of each class from your IDE.

**Typical Workflow:**
1.  **Start Kafka:** Ensure Zookeeper and Kafka Broker are running on `localhost:9092`.
2.  **Create Topic:** Run `TopicManager` to create `topicA`.
3.  **Start Consumer:** Run `BasicConsumer` (it will wait for messages).
4.  **Start Producer:** Run `BasicProducer` to send messages.
5.  **Observe:** Watch the console output of the Consumer to see messages arriving.

**Testing Transactions:**
1.  Run `TransactionalConsumer`.
2.  Run `TransactionalProducer`.
3.  Observe that the consumer only prints messages from committed transactions (e.g., even indices), skipping the aborted ones.
