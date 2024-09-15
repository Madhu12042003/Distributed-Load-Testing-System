import json
from kafka import KafkaProducer, KafkaConsumer
from flask import Flask, request, jsonify
from threading import Thread
import sys
from collections import defaultdict

app = Flask(__name__)

# Configuration
driver_nodes = defaultdict(dict)  # Store registered driver nodes (Error Handling)

# Kafka topics
kafka_bootstrap_servers = sys.argv[1] if len(sys.argv) > 1 else 'localhost:9092'  # Kafka broker address, defaulting to localhost
orchestrator_node_ip = sys.argv[2] if len(sys.argv) > 2 else 'http://127.0.0.1:5000' # Orchestrator node IP address, defaulting to localhost
topic1 = 'register'
topic2 = 'test_config'
topic3 = 'trigger'
topic4 = 'metrics'
topic5 = 'heartbeat'

# Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Kafka consumer
consumer1 = KafkaConsumer(topic1, topic2, topic3, topic4, topic5, bootstrap_servers='localhost:9092', api_version=(0, 11, 5), value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Register a new driver node
def register_driver_node(node_id, node_ip):
    driver_nodes[node_id]['node_ip']=node_ip      # To store ip address
    driver_nodes[node_id]['test_config'] = {}    # Store test configuration
    driver_nodes[node_id]['metrics'] = {}

# Configure and trigger a load test
def start_load_test(test_id, test_type, test_message_delay):
    for node_id in driver_nodes:
        
        driver_nodes[node_id]['test_config'] = {'test_id': test_id,'test_type': test_type, 'test_message_delay': test_message_delay}
        producer.send(topic2, value=driver_nodes[node_id])

        trigger_message = {'test_id': test_id, 'trigger': 'YES'}
        producer.send(topic3, value=trigger_message)

# Flask (Dynamic Routing)
@app.route('/')
def welc_pg():
    return 'Distributed Load Testing System'

@app.route('/register', methods=['POST'])
def register_node():
    data = request.get_json()
    node_id = data.get('node_id')
    node_ip = data.get('node_ip')
    register_driver_node(node_id, node_ip)
    return jsonify({'message': 'Node {node_id} is registered successfully'})

@app.route('/configure', methods=['POST'])
def configure_test():
    data = request.get_json()
    test_id = data.get('test_id')
    test_type = data.get('test_type')
    test_message_delay = data.get('test_message_delay')
    start_load_test(test_id, test_type, test_message_delay)
    return jsonify({'message': 'Test configuration sent to driver nodes'})


@app.route('/dashboard', methods=['GET'])
def get_dashboard():
    init_metrics={"Mean_latency":0,"Median_latency":0,"Min_latency":0,"Max_latency":0}
    no_of_nodes=len(driver_nodes)

    for node_id,metrics in driver_nodes.items():
        for metric,value in metrics['metrics'].items():
            init_metrics[metric]+=value
    
    if no_of_nodes>0:
        return jsonify({'dashboard': init_metrics})
    else:
        return jsonify({'dashboard': 'Not implemented'})

class Runtime_Contoller:
    def __init__(self):
        self.metrics_store=defaultdict(list)  # To store metric received by each node

    def check_and_handle_heartbeat(self,node_id,heartbeat_msg):
        print(f"Heartbeat received from node:{node_id} and status: {heartbeat_msg}")        

    def coordinate_load_testing(self,test_id,test_type,test_message_delay):
        start_load_test(test_id, test_type, test_message_delay)

    def receive_metrics(self,node_id,metrics):
        self.metrics_store[node_id].append(metrics)
        print(f"Metrics received from Node {node_id} is: {metrics}")  

# Kafka message listener
def kafka_listener():
    for message in consumer1:
        if message.topic == topic1:
            register_node(message.value['node_id'], message.value['node_ip'])

        elif message.topic == topic4:
            node_id = message.value['node_id']
            driver_nodes[node_id]['metrics'] = message.value['metrics']
            
        elif message.topic == topic5:
            node_id = message.value["node_id"]
            heartbeat_msg=message.value["heartbeat"]
            # print(f"Heartbeat received from node:{node_id} and status: {heartbeat_msg}")

        

if __name__ == '__main__':
    kafka_thread = Thread(target=kafka_listener)
    kafka_thread.daemon = True
    kafka_thread.start()

    app.run(host=orchestrator_node_ip, port=5000, debug=True)  # Start the Flask app

