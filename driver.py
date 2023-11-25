from kafka import KafkaProducer, KafkaConsumer
import time
import random
import statistics
import requests
import uuid
import sys
import json
from threading import Thread
#from requests_toolbelt.adapters import source

topic1 = 'register'
topic2 = 'test_config'
topic3 = 'trigger'
topic4 = 'metrics'
topic5 = 'heartbeat'


#active_nodes_list=[]

def send_request():
    start_time = time.time()
    _ = requests.get('http://127.0.0.1:8000/ping') # Sending GET request to target server
    end_time = time.time()
    latency = (end_time - start_time) * 1000  # Convert to milliseconds
    return latency

def send_metrics(node_id,test_config,latencies):
    msg={}
    msg["node_id"]=node_id
    msg["test_id"]=test_config["test_id"]
    msg["report_id"]=str(uuid.uuid4())
    msg["metrics"]={}
    msg["metrics"]["mean_latency"]=statistics.mean(latencies)
    msg["metrics"]["median_latency"]=statistics.median(latencies)
    msg["metrics"]["min_latency"]=min(latencies)
    msg["metrics"]["max_latency"]=max(latencies)
    kafka_producer.send(topic4,msg)
    print(msg)
    kafka_producer.flush()

def avalanche_test(node_id,test_config):
    latencies = []
    request_count = 0
    count=int(test_config["message_count_per_driver"])
    while request_count<count:
        latency = send_request()
        latencies.append(latency)
        request_count += 1
    send_metrics(node_id,test_config,latencies)
        
def tsunami_test(node_id,test_config):
    latencies = []
    request_count = 0
    delay=int(test_config["test_message_delay"])
    count=int(test_config["message_count_per_driver"])
    while request_count<count:
        latency = send_request()
        latencies.append(latency)
        request_count += 1
        time.sleep(delay)
    send_metrics(node_id,test_config,latencies)

def start_test(node_id,test_config):
    if(test_config["test_type"]=='AVALANCHE'):
        avalanche_test(node_id,test_config)
    else:
        tsunami_test(node_id,test_config)
        
def htbt(node_id):
    while(True):
        hb={}
        hb["node_id"]= node_id
        hb["heartbeat"]= "YES"
        hb["timestamp"]= time.time()
        #print(hb)
        kafka_producer.send(topic5,json.dumps(hb))
        time.sleep(2)
    
def kafka_listener(node_id):
    test_config={}
    trig={}
    print("Hello")
    for message in kafka_consumer:
        if message.topic == topic2:
            test_config=message.value
            print(test_config)
        if message.topic == topic3:
            trig=message.value
            print(trig)
            if(trig["trigger"]=="YES"):
                    start_test(node_id,test_config)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python driver.py <kafka_ip:port> ")
        sys.exit(1)
    kafka_ip = sys.argv[1]
    node_ip = sys.argv[2]
    # session = requests.Session()
    # session.source_address=node_id
    # Kafka producer for registration
    kafka_producer = KafkaProducer(bootstrap_servers=f'{kafka_ip}',value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Kafka consumer for commands
    kafka_consumer = KafkaConsumer(topic1,topic2,topic3,topic4,topic5, bootstrap_servers=f'{kafka_ip}', api_version=(0, 11, 5), value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Unique ID for the node
    node_id = str(uuid.uuid4())
    register_node={
        "node_id": node_id,
        "node_ip": node_ip,
        "message_type": "DRIVER_NODE_REGISTER"
    }
    try:
        print(register_node)
        kafka_producer.send(topic1,json.dumps(register_node))
    except Exception as e:
        print(str(e))
    kafka_producer.flush()
    
    heartbeat = Thread(target=htbt,args=(node_id,))
    heartbeat.daemon = True
    heartbeat.start()

    kafka_listener(node_id)