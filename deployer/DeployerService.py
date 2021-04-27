

import threading
from flask import Flask, jsonify, request
from kafka import KafkaProducer , KafkaConsumer

import json
import requests as rq
import time as t
import DeployerUpdate as dh
import time
import socket 
import pymongo
from pymongo import MongoClient

deployer_ip = 'deployer'
deployer_port = 5001
app = Flask(__name__)
appRepoIp 	= 'app_repo'
appRepoPort = 7007

cluster = MongoClient('mongodb://deployer_user:deployer@cluster0-shard-00-00.houot.mongodb.net:27017,cluster0-shard-00-01.houot.mongodb.net:27017,cluster0-shard-00-02.houot.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-frer5y-shard-0&authSource=admin&retryWrites=true&w=majority')
db = cluster['deployer_db']
collection = db['deployer_logs']

@app.route('/startdeployment', methods = ['POST', 'GET'])
def startDeployment():
	req = request.json

	#parse request from scheduler
	print("got request from scheduler")
	service_name, user_id, app_id, action_details, service_id, place_id = dh.handleRequest(req)
	log_service = {'state':'1', 'service_id':service_id, 'service_name':service_name, 'user_id':user_id, 'app_id':app_id, 'action_details':action_details, 'place_id':place_id}
	collection.insert_one(log_service)
	print("\n\b returned from handler")
	print("************after state 1************")
	#State 1 completed
	time.sleep(2)
	#request config file from app repo 

	config_json = dh.getConfig(app_id)
	collection.update_one({'service_id': service_id}, {'$set' : {'config_json':config_json, 'state':'2'}})
	print("\n\b returned from getconfig")
	print("************after state 2************")
	#state 2 completed
	time.sleep(2)
	#ip port from load balancer 
	container_name = dh.requestLoadBalancer(user_id,app_id,service_name, service_id)
	print("conatiner name to host application is ", container_name)
	collection.update_one({'service_id': service_id}, {'$set' : {'container_name':container_name, 'state':'3'}})
	print("\n\b returned from load balncer")

	machine_name, machinePassword = 'root', 'root'
	
	#extract service file
	service_file = dh.get_file_for_service(app_id, config_json, service_name)
	collection.update_one({'service_id': service_id}, {'$set' : {'service_file':service_file}})
	print("\n\b -----service file----")
	print("************after state 3************")
	#State 3 completed
	time.sleep(2)
	#collect sensor topic from sensor manager
	sensor_topics = dh.get_sensor_topic(config_json, user_id, service_name, service_id,app_id, place_id)
	collection.update_one({'service_id': service_id}, {'$set' : {'sensor_topics':sensor_topics, 'state':'4'}})
	print("\n\b ----got sensor topic-----")
	print("************after state 4************")
	#State 4 completed
	time.sleep(2)
	#application manager topic
	output_topic = 'action_service_topic'
	 
	# send request to app repo to copy code files at machine
	status = dh.SendFullRepo(machine_name, machinePassword, container_name, app_id, service_name, service_id)
	print("Status of ssh - --------------->", status.json()['status'])
	collection.update_one({'service_id': service_id}, {'$set' : {'state':'5'}})
	#State 5 Completed
	print("************after state 5************")
	time.sleep(2)
	#TODO: handle status as per app repo
	# status = status.json
	# print(status)
	# if(status['status'] == 'success'):
	print("success status")

	#deploy service on container
	pid = dh.deployeActual(machine_name, machinePassword, container_name, app_id, service_name, service_id, service_file, sensor_topics, output_topic)
	collection.update_one({'service_id': service_id}, {'$set' : {'pid': str(pid), 'state':'6'}})
	print("************after state 6************")
	# print("\n\b received continer id : ", container_id)

	#State 6 completed

	# container_val = container_id+'|'+machine_name+'|'+machinePassword+'|'+node_ip+'|'+node_port
	# algo_to_container_id[service_id]= container_val
	
	#send response to  Scheduler
	print("*******deployement completed*****")
	return jsonify({'status':'success'})


@app.route('/killed_service_update', methods = ['POST', 'GET'])
def killed_service_update():
	req = request.json
	user_id = req['user_id']
	app_id = req['app_id']
	service_name = req['service_name_to_run']
	service_id = req['service_id']

	#inform SensorManger
	dh.informSensorManager(service_id)

	#inform Scheduler
	dh.informScheduler(service_id)

	#Remove entry from mongoDB
	collection.remove({'service_id': service_id})

	print('Service killed')
	return jsonify({'status':'success'})


@app.route('/stopdeployment', methods = ['POST', 'GET'])
def stopDeployment():
  #TODO: Verify request coming from scheduler
	req = request.json
	user_id = req['user_id']
	app_id = req['app_id']
	service_name = req['service_name_to_run']
	service_id = req['service_id']

	#Inform load blancer (Not needed anymore) 
	# dh.logLoadBalancer(req)

	#inform SensorManger
	print("stop sent to sensor manager")
	dh.informSensorManager(service_id)
  
  
	#extract PID from mongo
	service_to_stop = collection.find({'service_id': service_id})
	service_to_stop = list(service_to_stop)
	for service in service_to_stop:
		pid = service['pid']
		container_name = service['container_name']

		#kill PID
		machine_name, machinePassword = 'root', 'root'
		dh.stopService(pid,container_name,machine_name,machinePassword)

		#Remove entry from mongoDB
		print("removing from DB")
		collection.remove({'service_id': service_id})
	
	print('Stopped service')
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



	
def initiateDeployer():
	app.run(host= '0.0.0.0', port = deployer_port, debug=False)	




if __name__ == '__main__':
	thread1 = threading.Thread(target = heartBeat)
	thread1.start()
	print("Start Deployer Server\n")
	thread2 = threading.Thread(target = initiateDeployer)
	thread2.start()
	dh.restartDeployer()
	# app.run(host=socket.gethostbyname(socket.gethostname()), port = deployer_port, debug=False)