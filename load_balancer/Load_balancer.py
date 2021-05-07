#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr  3 22:56:56 2021

@author: aman
"""
from flask import Flask,request, jsonify
from kafka import KafkaProducer , KafkaConsumer
import threading , json ,time
import pymongo 
import requests
import sys
import socket

dburl = "mongodb://apurva:user123@cluster0-shard-00-00.p4xv2.mongodb.net:27017,cluster0-shard-00-01.p4xv2.mongodb.net:27017,cluster0-shard-00-02.p4xv2.mongodb.net:27017/IAS_test_1?ssl=true&replicaSet=atlas-auz41v-shard-0&authSource=admin&retryWrites=true&w=majority"
db_name = "IAS_test_1"
myclient = pymongo.MongoClient(dburl)
mydb = myclient[db_name]
#appcol = mydb["application_log"]
hostcol = mydb["host_log"]
app = Flask(__name__)
app.config['DEBUG'] = True
@app.route("/return_host", methods=["GET", "POST"])
def returnServer():
	# get server with lowest request handling
	l = hostcol.find()     #get_hosts
	loads={}
	if l.count()==0:
	 return jsonify(
	 {"status" : 0}
	)
	for host in l:
		try:
			URL="http://"+host["name"]+":5000/get_load"
			r = requests.get(url = URL).json()
			tup=(r["mem_load"],r["cpu_load"])
			loads[host["name"]]=tup
		except:
			callfaultTolerance(host['name'])
			#print(loads,file=sys.stderr)    
	srt=sorted([(value,key) for (key,value) in loads.items()])
	print("Deploy on ",srt[0],file=sys.stderr)
	if srt==None or srt[0][0][0]>=100:
		return jsonify(
		   { "status" : 0}
		)
	return jsonify(
		{"machine_name" : srt[0][1]}
	)
# @app.route("/log_service", methods=["GET", "POST"])
# def logService():
 # # log in db, which application is running in which machine/server
 # # we require user_id, application_id, ip, port
 # content=request.json
 # #item to search
 # item={"app_id":content["app_id"],"user_id":content["user_id"],"service_name_to_run":content["service_name_to_run"],"service_id":content["service_id"]}
 
 # retr= appcol.find_one(item) #find item
 
 # if not retr:   #item not found
	 # item["ip"]=content["ip"]
	 # appcol.insert_one(item)         #new entry
 # else:          #item found
	# return jsonify(
		# status="0"
		# )

 # return jsonify(
 # status="1"
 # )


# @app.route("/free_service", methods=["GET", "POST"])
# def freeService():
 # # if a machine is not handling any request close it.
 # # we require user_id, application_id, ip, port
 # content=request.json
 # #item to search
 # item={"app_id":content["app_id"],"user_id":content["user_id"],"service_name_to_run":content["service_name_to_run"],"service_id":content["service_id"],"ip":content["ip"]}
 
 # retr= appcol.find_one(item) #find item
 
 # if not retr:   #item not found
	 # return jsonify(
		 # status="0"
		 # )
 
 # appcol.delete_one(item)
 # return jsonify(
 # status="1"
 # )

def json_deserializer(data):
	return json.dumps(data).decode('utf-8')

def json_serializer(data):
	return json.dumps(data).encode("utf-8")

def callfaultTolerance(module_name) :
	print(f"{module_name} FAILED !!!")
	to_send={}
	to_send['module_name'] = module_name

	json_object = json.dumps(to_send)
	fault_tolerance_url = 'http://host.docker.internal:6969/faulty'
	try :
		resp = requests.post(fault_tolerance_url, json = to_send)
		print(resp.text)
	except :
		print("Can't connect to Fault Tolerance Module")
	# communicate fault tolerance 
	
def heartBeat():
	kafka_platform_ip = 'host.docker.internal:9092'
	producer = KafkaProducer(bootstrap_servers=[kafka_platform_ip],value_serializer =json_serializer)
	while True:
		t = time.localtime()
		current_time = int (time.strftime("%H%M%S", t))
		# print(current_time)

		data = {"module" : "load_balancer" , "ts" : current_time , "Status" : 1   }
		producer.send("HeartBeat", data)
		producer.flush()
		time.sleep(3)


if __name__ == "__main__":
	thread1 = threading.Thread(target = heartBeat)
	thread1.start()
 # change to app.run(host="0.0.0.0"), if you want other machines to be able to reach the webserver.
	app.run(host='0.0.0.0',port=55555) 
 # app.run(host=socket.gethostbyname(socket.gethostname()),port=55555) 
