import json
import time
import random
import statistics
import requests
from kafka import KafkaProducer, KafkaConsumer
from flask import Flask, jsonify

app = Flask(__name__)

# Kafka topics
register_topic = 'register'
test_config_topic = 'test_config'
trigger_topic = 'trigger'
metrics_topic = 'metrics'
heartbeat_topic = 'heartbeat'

# Kafka bootstrap servers
kafka_bootstrap_servers = 'localhost:9092'

# Configuration
orchestrator_node_ip = 'http://127.0.0.1:5000'

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Create Kafka consumer for the Driver
consumer = KafkaConsumer(register_topic, test_config_topic, trigger_topic, bootstrap_servers=kafka_bootstrap_servers, api_version=(0, 11, 5), value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Function to register the driver node
def register_node(node_id, node_ip):
    registration_message = {
        'node_id': node_id,
        'node_ip': node_ip,
        'message_type': 'DRIVER_NODE_REGISTER'
    }
    print(registration_message)
    producer.send(register_topic, value=jsonify(registration_message))

# Function to simulate load testing
def simulate_load_test(node_id, target_server_url, delay_interval, target_throughput, message_count):
    test_config = {'test_id': 'test_' + str(random.randint(1, 100000)), 'test_type': 'TSUNAMI', 'test_message_delay': delay_interval, 'message_count_per_driver': message_count}
    producer.send(test_config_topic, value=test_config)

    trigger_message = {'test_id': test_config['test_id'], 'trigger': 'YES'}
    producer.send(trigger_topic, value=trigger_message)

    latencies = []
    start_time = time.time()

    while True:
        request_start_time = time.time()
        latency = send_request(target_server_url)
        latencies.append(latency)

        # Send metrics periodically
        if len(latencies) % 10 == 0:
            current_time = time.time()
            elapsed_time = current_time - start_time
            throughput = len(latencies) / elapsed_time

            # Check if the target throughput is reached
            if throughput >= target_throughput:
                break

            metrics = record_statistics(latencies)
            metrics_message = {
                'node_id': node_id,
                'test_id': test_config['test_id'],
                'report_id': str(random.randint(1, 100000)),
                'metrics': metrics
            }
            producer.send(metrics_topic, value=metrics_message)

        # Send heartbeat periodically
        if len(latencies) % 5 == 0:
            heartbeat_message = {
                'node_id': node_id,
                'heartbeat': 'YES',
                'timestamp': str(time.time())
            }
            producer.send(heartbeat_topic, value=heartbeat_message)

        time.sleep(delay_interval / 1000)  # Convert delay interval to seconds

# Function to send a request to the target web server
def send_request(target_server_url):
    response = requests.get(target_server_url)
    return response.elapsed.total_seconds() * 1000  # Convert to milliseconds

# Function to record statistics
def record_statistics(latencies):
    mean_latency = statistics.mean(latencies)
    median_latency = statistics.median(latencies)
    min_latency = min(latencies)
    max_latency = max(latencies)

    return {
        'mean_latency': mean_latency,
        'median_latency': median_latency,
        'min_latency': min_latency,
        'max_latency': max_latency
    }

if __name__ == '__main__':
    # Replace 'driver_node_1' with a unique identifier for this driver node
    node_id = 'driver_node_1'
    
    # Replace 'http://target_server_address/test_endpoint' with the actual target server URL
    target_server_url = 'http://127.0.0.1:8080'
    
    # Example: Simulate tsunami testing with a delay interval of 100 ms and a target throughput of 20 requests per second
    simulate_load_test(node_id=node_id, target_server_url=target_server_url,
                        delay_interval=100, target_throughput=20)

   
