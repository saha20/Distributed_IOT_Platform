import threading
from flask import Flask, jsonify, request
from kafka import KafkaProducer , KafkaConsumer

import json
import requests as rq
import time as t
# import pandas as pd
import DeployerUpdate as dh
import time
import socket 

deployer_ip = 'deployer'
deployer_port = 5001
algo_to_container_id = {}
app = Flask(__name__)
appRepoIp 	= 'app_repo'
appRepoPort = 7007

@app.route('/startdeployment', methods = ['POST', 'GET'])
def startDeployment():
	global algo_to_container_id
	req = request.json

	#parse request from scheduler
	print("got request from scheduler")
	service_name, user_id, app_id, action_details, service_id, longitude, latitude = dh.handleRequest(req)
	print("\n\b returned from handler")

	#request config file from app repo 

	config_json = dh.getConfig(app_id)
	print("\n\b returned from getconfig")

	#ip port from load balancer 
	node_ip = dh.requestLoadBalancer(user_id,app_id,service_name, service_id)
	print("\n\b returned from load balncer")

	machine_name, machinePassword = 'root', 'root'
	
	#extract service file
	service_file = dh.get_file_for_service(app_id, config_json, service_name)
	print("\n\b -----service file----")
	
	#collect sensor topic from sensor manager
	sensor_topics = dh.get_sensor_topic(config_json, user_id, service_name, service_id,app_id, longitude, latitude)
	print("\n\b ----got sensor topic-----")
	
	#application manager topic
	output_topic = 'action_service_topic'
	 
	# send request to app repo to copy code files at machine
	status = dh.SendFullRepo(machine_name, machinePassword, node_ip, app_id, service_name, service_id)

	#TODO: handle status as per app repo
	# status = status.json
	# print(status)
	# if(status['status'] == 'success'):
	print("success status")

	#deploy service on container
	dh.deployeActual(machine_name, machinePassword, node_ip, app_id, service_name, service_id, service_file)
	# print("\n\b received continer id : ", container_id)

	# container_val = container_id+'|'+machine_name+'|'+machinePassword+'|'+node_ip+'|'+node_port
	# algo_to_container_id[service_id]= container_val
	
	#send response to  SLM
	return jsonify({'status':'success'})

@app.route('/stopdeployment', methods = ['POST', 'GET'])
def stopDeployment():
	req = request.json
	user_id = req['user_id']
	app_id = req['app_id']
	service_name = req['service_name_to_run']
	service_id = req['service_id']

	#free server at load blancer
	# dh.freeServer(req)
	print('Stopped service')
	#TODO: send service id to Sensor manager, notify load balancer, fault tolerance, kill pid

	#Stop container
	# container_val = algo_to_container_id[service_id] 
	# dh.containerDelete(container_val,service_id)

	return jsonify({'status':'success'})


def json_deserializer(data):
    return json.dumps(data).decode('utf-8')

def json_serializer(data):
	return json.dumps(data).encode("utf-8")


def heartBeat():
	kafka_platform_ip ='kafka:9092'
	producer = KafkaProducer(bootstrap_servers=[kafka_platform_ip],value_serializer =json_serializer)
	while True:
		t = time.localtime()
		current_time = int (time.strftime("%H%M%S", t))
		# print(current_time)

		data = {"module" : "deployer" , "ts" : current_time , "Status" : 1   }
		producer.send("HeartBeat", data)
		producer.flush()
		time.sleep(3)
	


if __name__ == '__main__':
	thread1 = threading.Thread(target = heartBeat)
	thread1.start()
	print("Start Deployer Server\n")
	app.run(host= '0.0.0.0', port = deployer_port, debug=False)	
	# app.run(host=socket.gethostbyname(socket.gethostname()), port = deployer_port, debug=False)	