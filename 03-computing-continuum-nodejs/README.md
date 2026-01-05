# Computing Continuum with Node.js & Node-RED

## Overview
This module explores the **Computing Continuum** using **Node-RED** and **Node.js**. Node-RED is a flow-based programming tool, originally developed by IBM, for wiring together hardware devices, APIs, and online services in new and interesting ways.

It is heavily based on **JavaScript** and is widely used in IoT and Edge Computing scenarios to process data closer to the source before sending it to the cloud.


## ⚙️ Prerequisites & Setup

To experiment with these flows, you need to have a running instance of Node-RED.

### 1. Install Node-RED
The recommended way to run Node-RED is using **Docker**, as it isolates the environment and dependencies.

* **Docker (Recommended):**
    Follow the official guide: [Running Node-RED under Docker](https://nodered.org/docs/getting-started/docker).
    ```bash
    docker run -it -p 1880:1880 --name mynodered nodered/node-red
    ```

* **Native Installation (OS Specific):**
    If you have no experience with Docker or prefer a local installation, follow the instructions for your OS:
    * **Debian & Ubuntu:** [Raspberry Pi / Debian Guide](https://nodered.org/docs/getting-started/raspberrypi)
    * **RedHAT, CentOS, Fedora:** [Linux Installers](https://github.com/node-red/linux-installers)
    * **Windows:** [Windows Guide](https://nodered.org/docs/getting-started/windows)

### 2. Access the Editor
Once installed and running, access the Node-RED visual editor by opening your browser at:
`http://localhost:1880`

### 3. Import the Flows
The code in this repository consists of `.json` files representing Node-RED flows. To use them:
1.  Open the Node-RED Editor.
2.  Click the **Menu** (three lines in the top right) → **Import**.
3.  Select **Local** and upload the desired `.json` file from this folder.
4.  Click **Import** to add the flow to your workspace.

---

## Code Structure & Concepts

The examples are categorized by the architectural patterns and node types they demonstrate.

### 1. Basics & Logic Control
These flows introduce the basic blocks: Inject, Debug, and Function nodes (JavaScript logic).
* **`hello-world.json`**: A simple flow that triggers a message every 5 seconds, adds a timestamp, and prints it to the debug panel. It also includes an example of an email node.
* **`timestamp-even-odd.json`**: Demonstrates the **Function Node** (JS code) to route messages to different outputs based on logic. It checks if a timestamp is even or odd and routes it to the corresponding Debug node.

### 2. State Management
Node-RED is stateless by default. These examples show how to persist data.
* **`counter-node-context.json`**: Uses the internal **Node Context** (`context.get`/`context.set`) to maintain a counter variable in memory across multiple message executions.
* **`counter-file.json`**: Persists state to the disk using **File In** and **File Out** nodes. It reads a counter from a file, increments it, and saves it back.

### 3. Networking (UDP & MQTT)
Demonstrates communication protocols essential for the Computing Continuum.
* **`UDP-messages.json`**: Sends and receives UDP packets. It constructs a JSON payload (`greeting`, `timestamp`), serializes it, and sends it via UDP.
* **`UDP-echo-server.json`**: Implements a simple Echo Server that listens on port 5555, waits 1 second, and sends the received data back to the sender.
* **`MQTT.json`**: Subscribes to an MQTT topic (`neslabpolimi/smartcity/milan`). It processes sensor data (PM2.5, PM10, Temperature) to track maximum values using context variables.

### 4. External APIs & IoT Services
Integration with third-party REST APIs and services.
* **`open-weather-map.json`**: Queries the **OpenWeatherMap API** to get current weather data for Milan. It parses the JSON response to extract the temperature (`tempc`) and logs it to a file.
* **`telegram-bot.json`**: A conversational bot flow. It receives messages via Telegram, processes natural language (e.g., "What is the temperature in Milan?"), queries the weather API, and replies to the user.

---

## How to Run

1.  **Deploy:** After importing a JSON flow, click the red **Deploy** button in the top right corner to make the flow active.
2.  **Trigger:**
    * **Inject Nodes:** Click the square button on the left of an "Inject" node to manually start the flow.
    * **External Events:** For UDP, MQTT, or Telegram flows, send an actual packet/message to trigger the logic.
3.  **Observe:**
    * Open the **Debug Sidebar** (bug icon on the right) to see the output of `msg.payload`.

### Important Configuration Notes
Some flows require external credentials to work:
* **OpenWeatherMap:** You must double-click the `openweathermap` node and enter your valid API Key.
* **Telegram Bot:** You must configure the `chatbot-telegram-receive` node with your Bot Token (obtained from @BotFather).
* **Email:** The email node in `hello-world.json` requires valid SMTP server settings.
