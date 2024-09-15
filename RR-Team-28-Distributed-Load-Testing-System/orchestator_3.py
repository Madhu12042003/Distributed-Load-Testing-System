iimport json
import time
import random
from kafka import KafkaProducer, KafkaConsumer
from flask import Flask, request, jsonify
from threading import Thread
from collections import defaultdict
import statistics

app = Flask(__name__)

# Kafka topics
register_topic = 'register'
test_config_topic = 'test_config'
trigger_topic = 'trigger'
metrics_topic = 'metrics'
heartbeat_topic = 'heartbeat'

# Kafka bootstrap servers
kafka_bootstrap_servers = 'localhost:9092'

# Dictionary to store registered driver nodes
driver_nodes = defaultdict(dict)

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Create Kafka consumer for the Orchestrator
consumer = KafkaConsumer(register_topic, test_config_topic, metrics_topic, heartbeat_topic, bootstrap_servers=kafka_bootstrap_servers, api_version=(0, 11, 5), value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Function to register a new driver node
@app.route('/register', methods=['GET', 'POST'])
def register_driver_node():
    if request.method == 'POST':
        return jsonify({'message': 'Use POST method to register a node'})
    node_id = request.args.get('node_id')
    node_ip = request.args.get('node_ip')
    print(f"Received registration request. Node ID: {node_id}, Node IP: {node_ip}")
    driver_nodes[node_id]['node_ip'] = node_ip
    driver_nodes[node_id]['test_config'] = {}
    driver_nodes[node_id]['metrics'] = {}
    return jsonify({'message': f'Driver node {node_id} registered successfully'})


# Function to start a load test
def start_load_test(test_id, test_type, test_message_delay, message_count_per_driver):
    for node_id in driver_nodes:
        driver_nodes[node_id]['test_config'] = {'test_id': test_id, 'test_type': test_type, 'test_message_delay': test_message_delay, 'message_count_per_driver': message_count_per_driver}
        producer.send(test_config_topic, value=driver_nodes[node_id])

    trigger_message = {'test_id': test_id, 'trigger': 'YES'}
    producer.send(trigger_topic, value=trigger_message)

# Flask endpoint to view and control different tests
@app.route('/tests', methods=['GET', 'POST'])
def control_tests():
    if request.method == 'GET':
        return jsonify({'driver_nodes': dict(driver_nodes)})
    elif request.method == 'POST':
        data = request.get_json()
        test_id = data.get('test_id')
        test_type = data.get('test_type')
        test_message_delay = data.get('test_message_delay')
        message_count_per_driver = data.get('message_count_per_driver')
        start_load_test(test_id, test_type, test_message_delay, message_count_per_driver)
        return jsonify({'message': 'Load test triggered successfully'})

# Kafka message listener
def kafka_listener():
    for message in consumer:
        if message.topic == register_topic:
            register_driver_node(message.value['node_id'], message.value['node_ip'])

        elif message.topic == metrics_topic:
            node_id = message.value['node_id']
            driver_nodes[node_id]['metrics'] = message.value['metrics']

        elif message.topic == heartbeat_topic:
            node_id = message.value['node_id']
            heartbeat_msg = message.value['heartbeat']
            print(f"Heartbeat received from node: {node_id} and status: {heartbeat_msg}")

if __name__ == '__main__':
    kafka_thread = Thread(target=kafka_listener)
    kafka_thread.daemon = True
    kafka_thread.start()

    app.run(host='127.0.0.1', port=5000, debug=True)
