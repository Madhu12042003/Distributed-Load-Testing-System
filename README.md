# Distributed Load Testing System

## Overview

The Distributed Load Testing System is a framework designed for simulating load tests on a target server, managing the testing process, and aggregating performance metrics. It leverages **Kafka** for communication and **Flask** for exposing APIs. The system includes three main components:

1. **Driver Nodes**: Simulate load tests and send metrics and heartbeat messages to the orchestrator.
2. **Orchestrator**: Manages driver nodes, configures load tests, and aggregates metrics.
3. **Target Server**: A simple server that acts as the target for load testing.

## Architecture

- **Kafka**: Handles messaging between the orchestrator and driver nodes.
- **Flask (Orchestrator)**: Provides APIs for configuring and managing tests.
- **Driver Nodes**: Send requests to the target server, monitor latency, and report metrics and heartbeats.
- **Target Server**: Receives requests from driver nodes and provides a simple API for metrics.

## Components

### 1. `driver.py`

The `driver.py` script simulates load testing by sending requests to the target server and using Kafka for communication.

**Key Functions:**

- `send_request(target_server_url, start_time)`: Sends an HTTP GET request and calculates latency.
- `record_statistics(latencies)`: Calculates and records latency statistics (mean, median, min, max).
- `send_metrics(producer, topic4, node_id, test_id, metrics)`: Sends performance metrics to the orchestrator via Kafka.
- `send_heartbeat(producer, topic5, node_id)`: Sends a heartbeat message to the orchestrator.
- `simulate_load_test(node_id, target_server_url, delay_interval, target_throughput)`: Simulates load testing with specific parameters.
- `simulate_load_test(node_id, target_server_url, target_throughput)`: Alternative load testing simulation.

### 2. `orchestrator.py`

The `orchestrator.py` script manages the load testing process and aggregates metrics. It uses Flask to provide several API endpoints.

**Key Functions:**

- `register_driver_node(node_id, node_ip)`: Registers a driver node with its IP address.
- `start_load_test(test_id, test_type, test_message_delay)`: Configures and starts a load test for all driver nodes.
- `kafka_listener()`: Listens for Kafka messages related to node registration, metrics, and heartbeats.

**Flask API Endpoints:**

- `/register`: Register a new driver node. Expects a JSON payload with `node_id` and `node_ip`.
- `/configure`: Configure and start a load test. Expects a JSON payload with `test_id`, `test_type`, and `test_message_delay`.
- `/dashboard`: Retrieve aggregated metrics from all driver nodes.

### 3. `target_server.py`

The `target_server.py` script is a simple Flask server that acts as the target for load testing.

**Flask API Endpoints:**

- `/ping`: Simulates an interaction by returning a `"pong"` message and increments request and response counters.
- `/metrics`: Returns the number of requests and responses processed by the server.

## Kafka Configuration

Kafka is used for communication between the orchestrator and driver nodes. Ensure Kafka is running and properly configured.

**Kafka Topics:**

- `register`: For driver nodes to register themselves.
- `test_config`: Contains load test configuration details.
- `trigger`: Used to start load tests.
- `metrics`: Contains performance metrics from driver nodes.
- `heartbeat`: Periodic messages from driver nodes to indicate activity.

## Dependencies

The system requires the following Python packages:

- `Flask`
- `Kafka-Python`
- `requests`

Install these dependencies using your package manager of choice.

## Running the System

1. **Start Kafka**: Ensure the Kafka broker is up and running.
   
2. **Run the Target Server**: Start the target server to simulate the endpoint for load testing.

3. **Run the Orchestrator**: Start the orchestrator to manage driver nodes and coordinate load tests.

4. **Run the Driver Nodes**: Start one or more driver nodes to perform the load tests. Each driver node is configured with its ID, IP address, and test settings.

## Endpoints and Usage

### Orchestrator API

- **Register Node** (`/register`) [POST]: Registers a new driver node. The request body should be a JSON object with `node_id` and `node_ip`.

- **Configure Test** (`/configure`) [POST]: Configures and starts a load test. The request body should be a JSON object with `test_id`, `test_type`, and `test_message_delay`.

- **Get Dashboard** (`/dashboard`) [GET]: Retrieves aggregated metrics from all driver nodes.

### Target Server API

- **Ping** (`/ping`) [GET]: Returns a `"pong"` message and increments the request and response counters.

- **Metrics** (`/metrics`) [GET]: Returns the total number of requests and responses handled by the target server.

