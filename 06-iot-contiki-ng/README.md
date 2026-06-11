# Internet of Things with Contiki-NG

## Overview
This module explores **Internet of Things (IoT)** concepts using the **Contiki-NG** operating system, designed specifically for resource-constrained, low-power microcontrollers. It covers fundamental OS mechanisms like **Protothreads**, **Events**, and **Timers**, progressing to advanced networking protocols for the IoT, including **IPv6/UDP communication**, **RPL Routing**, and the **MQTT** application layer protocol.

## ⚙️ Prerequisites & Setup

To compile and simulate these Contiki-NG programs, you need the official toolchain or the pre-configured Virtual Machine.

### 1. Install the Environment
We recommend using the customized Virtual Machine provided for the course, which has all dependencies pre-installed.
* **Download VirtualBox:** Install VirtualBox and its Extension Pack.
* **Download the VM:** Get the provided VM image (or use a compatible Linux environment).
* **Credentials:** `user` / `user`

### 2. Project Setup & Compilation
1. **Clone the Repository:**
    ```bash
    git clone https://github.com/cienzman/networked-software-tutorials.git
    cd networked-software-tutorials/06-iot-contiki-ng
    ```
2. **Compile an Example (Native):** Compile an application to run directly on your host OS:
    ```bash
    cd examples/hello-world
    make TARGET=native
    ```
3. **Run the Application:**
    ```bash
    ./hello-world.native
    ```

---

## Code Structure & Concepts

The examples are categorized by architectural patterns and communication strategies, similar to the HPC module.

### 1. Core OS Concepts & Concurrency
These files demonstrate how Contiki-NG handles concurrency without a preemptive scheduler using **Protothreads** and **Events**.
* **`producer-consumer`**: Demonstrates inter-process communication using custom events. It shows how a producer and consumer process yield execution and wake each other up using `process_post()` and `PROCESS_WAIT_EVENT_UNTIL()`.
* **`timers_c_vs_r`**: Explores Contiki-NG's timer abstractions.
    * **`ctimer` (Callback Timer):** Triggers a specific callback function upon expiration.
    * **`rtimer` (Real-time Timer):** Schedules tasks at exact times, providing strict timing guarantees.

### 2. Networking & Routing (IPv6, UDP, RPL)
These directories focus on wireless sensor network communication using the **Routing Protocol for Low-Power and Lossy Networks (RPL)**.
* **`rpl-udp_pingpong`**: A simple application where nodes exchange UDP ping-pong messages to demonstrate basic point-to-point communication.
* **`rpl-udp_temperature`**: Uses the `simple-udp` API to send simulated temperature readings from leaf nodes (clients) to the root of the RPL Directed Acyclic Graph (server).
* **`rpl-border-router`**: Implements an RPL Border Router. This crucial component sits at the edge of the wireless sensor network and bridges it to standard external IP networks (like the Internet), allowing external devices to communicate with the constrained nodes.

### 3. Application Protocols (MQTT)
* **`mqtt-demo`**: An implementation of an MQTT client over TCP/IP. It establishes a connection to an MQTT broker, handles re-connections via a state machine, publishes simulated temperature and uptime data (often formatted as JSON), and subscribes to command topics.

---

## How to Run

You can run Contiki-NG applications natively (as Linux processes) or simulate them using the **COOJA** network simulator.

1. **Navigate** to a specific example directory:
    ```bash
    cd examples/producer-consumer
    ```
2. **Compile and Execute Natively:**
    ```bash
    make TARGET=native consumer-producer
    ./consumer-producer.native
    ```

**Using the COOJA Simulator:**
For networking examples (`rpl-udp_*`, `mqtt-demo`), you should simulate multiple nodes.
1. Start COOJA from the `tools/cooja` directory:
    ```bash
    cd ../../tools/cooja
    ant run
    ```
2. In COOJA, go to **File -> New simulation**.
3. Go to **Motes -> Add motes -> Create new mote type -> Cooja mote**.
4. Browse to the C file (e.g., `examples/rpl-udp_temperature/udp-client.c`), compile, and add multiple nodes to the network canvas.

**Testing the Border Router & MQTT:**
1. Deploy `border-router.c` in COOJA as Node 1. Right-click the node -> **Mote tools -> Serial socket (SERVER)**.
2. In a terminal, run the bridge script to connect your host to the simulated network:
    ```bash
    make TARGET=cooja connect-router-cooja
    ```
3. Deploy the MQTT client (`mqtt-demo.c`) as Node 2.
4. Use `mosquitto_sub` on your host to observe published data:
    ```bash
    mosquitto_sub -h localhost -p 1883 -t "#" -v
    ```
