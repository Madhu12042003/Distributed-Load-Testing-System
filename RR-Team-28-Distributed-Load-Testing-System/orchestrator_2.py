#!/usr/bin/env python3

import json
import random
from flask import Flask, request, jsonify
from kafka import KafkaProducer, KafkaConsumer
from threading import Thread

app = Flask(__name__)

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

driver_nodes = {}

def generate_test_id():
    return str(random.randint(1, 100000))

def send_test_config_message(test_type, test_message_delay, message_count_per_driver):
    test_id = generate_test_id()
    test_config_message = {
        'test_id': test_id,
        'test_type': test_type,                       # Test_type, test_message_delay, message_count_per_driver  ->  should be taken from Targer HTTP Server
        'test_message_delay': test_message_delay,
        'message_count_per_driver': message_count_per_driver
    }
    kafka_producer.send('test_config', key=test_id, value=json.dumps(test_config_message).encode('utf-8'))

    trigger_message = {'test_id': test_id, 'trigger': 'YES'}
    kafka_producer.send('trigger', key=test_id, value=json.dumps(trigger_message).encode('utf-8'))

def get_metrics():
    # Aggregate metrics from driver nodes
    aggregate_metrics = {
        'mean_latency': 0,
        'median_latency': 0,
        'min_latency': 0,
        'max_latency': 0
    }

    for node_id, metrics in driver_nodes.items():
        for metric, value in metrics['metrics'].items():
            aggregate_metrics[metric] += value

    return aggregate_metrics

@app.route('/')
def welcome():
    return 'Distributed Load Testing System'


@app.route('/dashboard', methods=['GET'])
def get_dashboard():
    aggregate_metrics = get_metrics()
    return jsonify({'dashboard': aggregate_metrics})


def kafka_listener():
    kafka_consumer.subscribe(['register', 'test_config', 'trigger', 'metrics', 'heartbeat'])

    for msg in kafka_consumer:
        topic = msg.topic
        value = json.loads(msg.value.decode('utf-8'))

        if topic == 'register':
            node_id = value['node_id']
            node_ip = value['node_ip']
            driver_nodes[node_id] = {'node_ip': node_ip, 'test_config': {}, 'metrics': {}}
            print(f"Node {node_id} is registered successfully")

        elif topic == 'metrics':
            node_id = value['node_id']
            test_id = value['test_id']
            driver_nodes[node_id]['metrics'] = value['metrics']

if __name__ == '__main__':
    # send_test_config_message("AVALANCHE",5,20)       As discussed, above in the comment... 
    kafka_thread = Thread(target=kafka_listener)
    kafka_thread.daemon = True
    kafka_thread.start()

    app.run(host='127.0.0.1', port=5000, debug=True)
