import threading
from flask import Flask, jsonify, request
from kafka import KafkaProducer , KafkaConsumer

import json
import requests as rq
import time
import pymongo
from pymongo import MongoClient

app = Flask(__name__)
service_lm_port = 8089

services_dict = {}
deployer_service_url = 'http://deployer:5001'

# cluster = MongoClient('mongodb://deployer_user:deployer@cluster0-shard-00-00.houot.mongodb.net:27017,cluster0-shard-00-01.houot.mongodb.net:27017,cluster0-shard-00-02.houot.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-frer5y-shard-0&authSource=admin&retryWrites=true&w=majority')
cluster = MongoClient('mongodb://apurva:user123@cluster0-shard-00-00.p4xv2.mongodb.net:27017,cluster0-shard-00-01.p4xv2.mongodb.net:27017,cluster0-shard-00-02.p4xv2.mongodb.net:27017/IAS_test_1?ssl=true&replicaSet=atlas-auz41v-shard-0&authSource=admin&retryWrites=true&w=majority')
db = cluster['IAS_test_1']
collection = db['deployer_logs']


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
		data = {"module" : "service_lcm" , "ts" : current_time , "Status" : 1   }
		producer.send("HeartBeat", data)
		producer.flush()
		time.sleep(3)

@app.route('/listen_services_heartbeat', methods = ['POST', 'GET'])
def listen_services_heartbeat():
	global services_dict
	req = request.json
	service_id = req['service_id']
	print("received heartbeat from -------", service_id)
	if(service_id in services_dict.keys()):
		services_dict[service_id]+=1
	else:
		services_dict[service_id]=1
	print("updated in map for id ---", service_id)
	return jsonify({'status':'success'})




def identify_services_status():
	while(1):
		time.sleep(30)
		
		# fetch current running services on deployer from DB
		running_services = collection.find()

		print("map in SLM")
		print(services_dict)
		running_services_list = list(running_services)
		print(running_services_list)
		print("all ids in db -- ", collection.distinct('service_id'))
		db_sids = list(collection.distinct('service_id'))
		for service in running_services_list:
			print("in for loop", service)
			if service['state']=='6':
				s_id = service['service_id']
				if(s_id not in services_dict.keys() or services_dict[s_id] == 0):
					
					req = {}
					req['user_id'] 			= service['user_id']
					req['app_id']  			= service['app_id']
					req['service_name_to_run'] = service['service_name']
					req['service_id']		 = s_id

					#notify deployer about the killed service
					print("found killed service -- ", s_id)
					try:
						res = rq.post(url = deployer_service_url+'/killed_service_update', json = req)
						if s_id in services_dict.keys():
							del services_dict[s_id]
					except:
						pass

				else:
					print("setting sid 0")
					services_dict[s_id] = 0

		sids_in_dict = 	list(services_dict.keys())	
		print("list of sids in dict -- ", sids_in_dict)	
		for s in sids_in_dict:
			if s not in db_sids:
				del services_dict[s]
		print("now dict is ", services_dict)
		print("outsie for loop")




def initiate_listen_service():
	app.run(host= '0.0.0.0', port = service_lm_port, debug=False)	


if __name__ == '__main__':
	thread1 = threading.Thread(target = heartBeat)
	thread1.start()
	print("Start Deployer Server\n")
	thread2 = threading.Thread(target = initiate_listen_service)
	thread2.start()
	# time.sleep(10)
	thread3 = threading.Thread(target = identify_services_status)
	thread3.start()
	# app.run(host=socket.gethostbyname(socket.gethostname()), port = deployer_port, debug=False)	