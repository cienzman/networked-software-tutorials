# Concurrent Systems with Akka

## ⚙️ Prerequisites & Setup

To run the examples in this module, please ensure your environment is set up as follows:

1.  **Install Maven:**
    * Download and install Maven if it is not already running on your machine.
    * [**Installation Guide**](https://maven.apache.org/install.html)

2.  **Verify Akka Installation:**
    * Check the [**Akka Quickstart Guide for Java**](https://developer.lightbend.com/guides/akka-quickstart-java/).
    * Ensure you can run the *Hello World* example provided in the guide (at least from the command line) to verify your Java and Akka dependencies are working correctly.

3.  **Get the Source Code:**
    * You can Clone this repository and consider only this sub-directory
        ```bash
        git clone https://github.com/cienzman/networked-software-tutorials.git
        ```
    * Consider only this sub-directory: 01 Concurrent Systems (Akka)

4.  **Import into IDE:**
    * Import the downloaded code into the Java IDE of your choice (IntelliJ IDEA, Eclipse, VS Code).
    * *For Eclipse users:* Click on `File` -> `Import` -> `Maven` -> `Existing Maven Projects`.

---

## Overview
This module explores **Concurrent Systems** using the **Akka** framework (Java). The goal is to understand the **Actor Model**, a conceptual model for dealing with concurrent computation where "actors" are the universal primitives.

The examples in this folder demonstrate:
1.  **Thread-safe State Management:** How actors handle mutable state (a counter) without explicit locks.
2.  **Fault Tolerance:** The "Let it crash" philosophy using Supervisor Strategies.
3.  **Message Passing:** Asynchronous communication between actors.

## 📂 Code Structure

### 1. Basic Counter (Concurrency)
* **`CounterActor.java`**: A simple actor that holds an internal counter variable. It reacts to `IncreaseMessage` and `DecreaseMessage` to modify its state.
* **`Counter.java`**: The main entry point. It creates a thread pool to send messages concurrently to the actor, demonstrating that the actor processes messages sequentially, preserving data integrity without `synchronized` blocks.

### 2. Fault Tolerance (Supervision)
* **`CounterSupervisorActor.java`**: Implements a `OneForOneStrategy`. It supervises a child actor and decides what to do if the child fails (e.g., `Restart`, `Resume`, or `Stop`).
* **`CounterSupervisor.java`**: The main entry point that triggers a fault. It sends a `FAULT_OP` code via a `DataMessage` to the child, causing an exception to test the supervisor's reaction.

### 3. Messages
* **`SimpleMessage`, `IncreaseMessage`, `DecreaseMessage`**: Signal messages used to trigger actions.
* **`DataMessage`**: Used to carry data payload (operation codes).
