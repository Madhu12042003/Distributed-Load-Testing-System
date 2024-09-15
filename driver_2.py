#!/usr/bin/env python3

import json
import time
import random
import statistics
from kafka import KafkaProducer,KafkaConsumer
import sys
import requests

# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092'
kafka_producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)
kafka_consumer = KafkaConsumer(
    'register',
    'test_config',
    'trigger',
    'metrics',
    'heartbeat',
    bootstrap_servers=kafka_bootstrap_servers,
)

# Configuration
orchestrator_kafka_bootstrap_servers = 'localhost:9092'
topic1= 'register'
topic4 = 'metrics'
topic5 = 'heartbeat'

def send_register_message(node_id, node_ip):
    register_message = {
        "node_id": node_id,
        "node_ip": node_ip,
        "message_type": "DRIVER_NODE_REGISTER"
    }

    kafka_producer.send(topic1, key=node_id.encode('utf-8'), value=json.dumps(register_message).encode('utf-8'))


# Function to send a request to the target web server
def send_request(target_server_url, start_time):
    response = requests.get(target_server_url) # Sending GET request to target server
    end_time = time.time()
    latency = (end_time - start_time) * 1000  # Convert to milliseconds
    return latency

# Function to record statistics
def record_statistics(latencies):
    mean_latency = statistics.mean(latencies)
    median_latency = statistics.median(latencies)
    min_latency = min(latencies)
    max_latency = max(latencies)

    return {
        "mean_latency": mean_latency,
        "median_latency": median_latency,
        "min_latency": min_latency,
        "max_latency": max_latency,
    }

# Function to send metrics to the Orchestrator node
def send_metrics(node_id, test_id, metrics):
    metrics_message = {
        "node_id": node_id,
        "test_id": test_id,
        "report_id": str(random.randint(1, 100000)),
        "metrics": metrics,
    }
    kafka_producer.send(topic4, key=test_id.encode('utf-8'), value=json.dumps(metrics_message).encode('utf-8'))

# Function to send heartbeats to the Orchestrator node
def send_heartbeat(node_id):
    heartbeat_message = {
        "node_id": node_id,
        "heartbeat": "YES",
        "timestamp": str(time.time())
    }
    kafka_producer.send(topic5, key=node_id.encode('utf-8'), value=json.dumps(heartbeat_message).encode('utf-8'))

# Avalanche Load testing
def simulate_avalanche_load_test(node_id, test_id, target_server_url, message_count_per_driver, target_throughput):
    latencies = []
    request_count = 0

    while request_count < message_count_per_driver:
        request_start_time = time.time()
        latency = send_request(target_server_url, request_start_time)
        latencies.append(latency)

        # Send metrics for every request
        metrics = record_statistics(latencies)
        send_metrics(node_id, test_id, metrics)

        # Send heartbeat for every request
        send_heartbeat(node_id)

        # Check if the target throughput is reached
        request_count += 1
        throughput = request_count / (time.time() - request_start_time)

        if throughput >= target_throughput:
            break

    # Wait for a short period to allow metrics and heartbeats to be sent
    time.sleep(2)

def simulate_tsunami_load_test(node_id,test_id,target_server_url, delay_interval,message_count_per_driver, target_throughput):

    latencies = []
    start_time = time.time()
    request_count = 0
    
    while request_count < message_count_per_driver:
        request_start_time = time.time()
        latency = send_request(target_server_url,request_start_time)
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
            send_metrics(node_id, test_id , metrics)

        # Send heartbeat periodically
        if len(latencies) % 5 == 0:
            send_heartbeat(node_id)

        time.sleep(delay_interval / 1000)  # Convert delay interval to seconds

def kafka_listeners():
    kafka_consumer.subscribe(['register', 'test_config', 'trigger', 'metrics', 'heartbeat'])

    for msg in kafka_consumer:
        topic = msg.topic
        value = json.loads(msg.value.decode('utf-8'))

        if topic == 'test_config':
            global test_id,test_type,test_message_delay,message_count_per_driver
            test_id=value["test_id"]
            test_type=value["test_type"]
            test_message_delay=value["test_message_delay"]
            message_count_per_driver=value["message_count_per_driver"]

        elif topic == 'trigger':
            global trigger
            trigger=value["trigger"]

if __name__ == '__main__':
    node_id=sys.argv[1]
    node_ip=sys.argv[2]
    send_register_message(node_id,node_ip)

    kafka_listeners()

    if trigger=="YES":
        if test_type=="AVALANCHE":
            simulate_avalanche_load_test(node_id,test_id,'http://target_server_address/ping',message_count_per_driver,target_throughput=20)

        elif test_type=="TSUNAMI":
            simulate_tsunami_load_test(node_id,test_id,'http://target_server_address/ping',test_message_delay,message_count_per_driver,target_throughput=20)

