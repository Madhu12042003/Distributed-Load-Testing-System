import json
import time
import random
import statistics
import requests
import sys
from kafka import KafkaProducer, KafkaConsumer

# Configuration
orchestrator_kafka_bootstrap_servers = 'localhost:9092' 
topic1 = 'register'
topic2 = 'test_config'
topic3 = 'trigger'
topic4 = 'metrics'
topic5 = 'heartbeat'

producer = KafkaProducer(bootstrap_servers=orchestrator_kafka_bootstrap_servers,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

consumer1 = KafkaConsumer(topic2, topic3, bootstrap_servers='localhost:9092', api_version=(0, 11, 5), value_deserializer=lambda x: json.loads(x.decode('utf-8')))


# Function to send a request to the target web server
def send_request(target_server_url,start_time):
    response = requests.get(target_server_url)
    end_time = time.time()
    latency = (end_time - start_time) * 1000  # Convert to milliseconds (It is difficult to see changes in seconds)
    return latency

# Function to record statistics
def record_statistics(latencies):
    mean_latency = statistics.mean(latencies)
    median_latency = statistics.median(latencies)
    min_latency = min(latencies)
    max_latency = max(latencies)

    return {
        "Mean_latency": mean_latency,
        "Median_latency": median_latency,
        "Min_latency": min_latency,
        "Max_latency": max_latency,
    }

# Function to send metrics to the Orchestrator node
def send_metrics(producer, topic4, node_id, test_id, metrics):
    metrics_message = {
        "node_id": node_id,
        "test_id": test_id,
        "report_id": str(random.randint(1, 100000)),  # Replace with an actual unique identifier
        "metrics": metrics,
    }
    producer.send(topic4, value=metrics_message)

# Function to send heartbeats to the Orchestrator node
def send_heartbeat(producer, topic5, node_id):
    heartbeat_message = {
        "node_id": node_id,
        "heartbeat": "Active",
    }
    producer.send(topic5, value=heartbeat_message)

# Function to simulate load testing
def simulate_load_test(node_id, target_server_url, delay_interval, target_throughput):

    latencies = []
    start_time = time.time()

    while True:
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
            send_metrics(producer, topic4, node_id, driver_nodes[node_id]['test_config']['test_id'] , metrics)

        # Send heartbeat periodically
        if len(latencies) % 5 == 0:
            send_heartbeat(producer, topic5, node_id)

        time.sleep(delay_interval / 1000)  # Convert delay interval to seconds

def simulate_load_test(node_id, target_server_url, target_throughput):

    latencies = []
    request_count = 0

    while True:
        request_start_time = time.time()
        latency = send_request(target_server_url)
        latencies.append(latency)

        # Send metrics for every request
        metrics = record_statistics(latencies)
        send_metrics(producer, topic4, node_id, driver_nodes[node_id]['test_config']['test_id'], metrics)

        # Send heartbeat for every request
        send_heartbeat(producer, topic5, node_id)

        # Check if the target throughput is reached
        request_count += 1
        throughput = request_count / (time.time() - request_start_time)

        if throughput >= target_throughput:
            break

    # Wait for a short period to allow metrics and heartbeats to be sent
    time.sleep(2)


# Kafka message listener
def kafka_listener():
    for message in consumer1:
        if message.topic == topic2:
            global driver_nodes
            driver_nodes=message.value

        if message.topic == topic3:
            if message.value['trigger']=='YES':
                return True
            else:
                return False

def register_node(nid,nip):
    node_registration = {
        "node_id": nid,
        "node_ip": nip,
    }
    producer.send(topic1, value=node_registration)

def test_config(test_id,test_type,test_message_delay,node_id):
    test_config = {
        "test_id": test_id,
        "test_type": test_type,
        "test_message_delay":test_message_delay,
        "node_id":node_id,
    }
    producer.send(topic2, value=test_config)
        

if __name__ == '__main__':
    if 3> len(sys.argv)> 6:
        sys.exit(1)
    node_id = sys.argv[1]
    node_ip = sys.argv[2]
    test_type= sys.argv[3]
    test_message_delay=sys.argv[4]

    register_node(node_id,node_ip)

    test_id=random.randint(1, 100000)
    test_config(test_id,test_type,test_message_delay,node_id)

    x=kafka_listener()

    if x:
        if driver_nodes[node_id]['test_config']['test_type']=="AVALANCHE":# Example: Simulate tsunami testing with a delay interval of 100 ms and a target throughput of 20 requests per second
            simulate_load_test(node_id=node_id,target_server_url='http://target_server_address/test_endpoint',
                            delay_interval=test_message_delay, target_throughput=20)
        else:
            simulate_load_test(node_id=node_id, target_server_url='http://target_server_address/test_endpoint',
                   target_throughput=20)
