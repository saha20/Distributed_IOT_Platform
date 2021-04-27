

import threading
from flask import Flask, jsonify, request
import json
import requests as rq
import time as t

import json
from pathlib import Path
from kafka import KafkaConsumer, KafkaProducer
import os
import paramiko
import pymongo
from pymongo import MongoClient

#appRep 
appRepo_url = 'http://app_repo:7007'
#load balancer ip port 
lb_url = 'http://load_balancer:5012'
#sensor manager url
sm_url = "http://sensor_manager:5012"
#scheduler url TODO: ask port
sch_url = "http://scheduler:5012"

#kafka_ip
KAFKA_IP = 'kafka:9092'

cluster = MongoClient('mongodb://deployer_user:deployer@cluster0-shard-00-00.houot.mongodb.net:27017,cluster0-shard-00-01.houot.mongodb.net:27017,cluster0-shard-00-02.houot.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-frer5y-shard-0&authSource=admin&retryWrites=true&w=majority')
db = cluster['deployer_db']
collection = db['deployer_logs']


# def containerDelete(container_details,service_id):
# 	container_details = container_details.split('|')
# 	container_id, machineName, machinePassword, node_ip, node_port = container_details[0],container_details[1],container_details[2],container_details[3],container_details[4]

# 	ssh_client = paramiko.SSHClient()
# 	ssh_client.load_system_host_keys()
# 	ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
# 	ssh_client.connect(hostname=node_ip, username=machineName,password=machinePassword)
# 	#TODO: remove service pid from container n keep a map for same
# 	# ssh_client.exec_command('echo root | sudo docker container rm -f '+container_id)
# 	# ssh_client.exec_command('echo root | sudo rm -f '+ " '"+service_id+"'")
# 	# os.system('sudo docker container rm -f '+container_id)
# 	# os.system('sudo docker container rm -f '+container_id)

def logLoadBalancer(req):
	status = rq.post(lb_url+'/free_server', json = req)

def informSensorManager(service_id):
	print("here in inform sensor manager")
	req = {}
	req['service_id'] = service_id
	# rsp = rq.post(lb_url+'/return_host', json = req)
	rsp = rq.post(sm_url+'/stopService', json = req)
	print("received response---",rsp)
 
def informScheduler(service_id):
	req = {}
	req['service_id'] = service_id
	rsp = rq.post(sch_url+'/stopService', json = req)
	print("returned from sensor manager")
 
def stopService(pid,node_ip,machineName,machinePassword):
	ssh_client = paramiko.SSHClient()
	ssh_client.load_system_host_keys()
	ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	print("node ip ", node_ip)
	print("pid ", pid)
	print("machineName", machineName)
	print("machinePassword", machinePassword)
	ssh_client.connect(hostname=node_ip, username=machineName,password=machinePassword)
	ssh_client.exec_command('mkdir "101"')
	ssh_client.exec_command('kill -9 '+str(pid)) #TODO : if sudo works in ssh
	print("killed process")

def deployeActual(machineName, machinePassword, node_ip, app_id, service_name, service_id, service_file,temp_topic,output_topic):
	ssh_client = paramiko.SSHClient()
	ssh_client.load_system_host_keys()
	ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	ssh_client.connect(hostname=node_ip, username=machineName,password=machinePassword)


	#run service script file
	filepath = './'+service_id+'/'+service_file
	print("running file", filepath)
	stdin,stdout,stderr=ssh_client.exec_command('python3 '+filepath+' '+temp_topic+' '+output_topic+' '+service_id)
	# stdin,stdout,stderr=ssh_client.exec_command('echo root | python3 -u '+filepath)
	# print(stdout.readlines())
	# print(stderr.readlines())
	print("script running succesffully")
	# print("pgrep -f "+ filepath")
	stdin,stdout,stderr = ssh_client.exec_command('echo root | pgrep -f '+ filepath)
	pids = list(stdout.readlines())[0].strip()
	print("process id is ---- ", pids)
	# return stdout.readlines()[0]
	return pids



def SendFullRepo(machineName, machinePassword, machineIp, app_id, serviceName, service_id):
	req = {}
	req['machineName'] 		= machineName
	req['machinePassword']  = machinePassword
	req['machineIp']		= machineIp
	req['app_id']			= app_id
	req['serviceName'] 		= serviceName
	req['service_id'] 		= service_id
	# req['sensor_topic']		= sensor_topics
	# req['output_topic']		= 'action_service_topic'
	res = rq.post(url = appRepo_url+'/send_files_machine', json = req)
	print('sent request to app repo')
	return res


def getConfig(app_id):
	req = {}
	req['app_id'] = app_id
	config_json = rq.post(appRepo_url+'/send_config_file', json = req)
	config_json = config_json.json()
	return config_json



def get_sensor_topic(config_json, user_name, service_name,service_id,app_id, place_id):
	application_name = config_json[app_id]['application_id']
	sensor_topics = []
	
	
	
	req = {
		'username' : user_name,
		'applicationname' : application_name,
		'servicename' : service_name,
		'serviceid' : service_id,
		'place_id' : place_id,
		'config_file' : config_json
	}
	res = rq.post(url = sm_url+'/sensorManagerStartService', json = req)
	print(res.json())
	return res.json()['temporary_topic']


def get_file_for_service(app_id, config_json, service_name):
	# filename = open(config_filepath, 'r')
	# config_json = json.load(filename)
	# filename.close()
	services = config_json[app_id]['algorithms']
	for k,v in services.items():
		if k == service_name:
			return v['code']
	return "Incorrect Service name"

def handleRequest(req):
	print("in requets handler --- \n",req)
	user_id = req['user_id']
	app_id = req['app_id']
	service_name = req['service_name_to_run']
	action_details = req['action']
	service_id = req['service_id']
	place_id = req['place_id']
		
	print("returning from req handler")
	return service_name, user_id, app_id, action_details, service_id, place_id


def requestLoadBalancer(user_id, app_id, service_name, service_id):

	#request ip port
	print("\n\b in request Load balancer")

	lb_response = rq.post(lb_url+'/return_host', json = "")
	lb_response = lb_response.json()
	node_ip = lb_response['ip']
	# node_port = lb_response['port']
	print("Got Load balancer ip port")

	###########################################################
	# #log service to load balancer 
	# req = {}
	# req['app_id'] = app_id
	# req['user_id'] = user_id
	# req['service_name_to_run'] = service_name
	# req['service_id'] = service_id
	# rsp = rq.post(lb_url+'/log_application', json = req)
	print("\n\b sent request in load balancer")
	return node_ip

def initActionManager(action_details,user_id, app_id, service_name, output_topic, service_id):
	action_dict = action_details.load()
	action_dict['user_id'] = user_id
	action_dict['app_id'] = app_id
	action_dict['service_name'] = service_name
	action_dict['service_id'] = service_id
	# action_dict['output_topic'] = 'to_action_manager'
	action_details = json.dumps(action_dict)
	producer = KafkaProducer(bootstrap_servers=KAFKA_IP,value_serializer=lambda v: json.dumps(v).encode('utf-8'))
	producer.send('action_manager',action_details)



def restartDeployer():
	
	print("in restart mode")

	list_of_services = collection.find()
	list_of_services = list(list_of_services)
	print(list_of_services)

	for service in list_of_services:
		service_id = service['service_id']
		state = service['state']
		service_name = service['service_name']
		user_id = service['user_id']
		app_id =  service['app_id']
		action_details =  service['action_details']
		place_id =  service['place_id']
		config_json,  container_name, service_file, sensor_topics = '', '', '', ''
		print("success till 1")

		if(int(state) < 2):
			print("failed at 2")
			config_json = getConfig(app_id)
			collection.update_one({'service_id': service_id}, {'$set' : {'config_json':config_json, 'state':'2'}})
		else:
			config_json = service['config_json']

		print("success till 2")

		if(int(state) < 3):
			print("failed at 3")
			container_name = requestLoadBalancer(user_id,app_id,service_name, service_id)
			collection.update_one({'service_id': service_id}, {'$set' : {'container_name':container_name, 'state':'3'}})
			service_file = get_file_for_service(app_id, config_json, service_name)
			collection.update_one({'service_id': service_id}, {'$set' : {'service_file':service_file}})
		else:
			container_name = service['container_name']
			service_file =  service['service_file']

		print("success till 3")

		if(int(state) < 4):
			print("failed at 4")
			sensor_topics = get_sensor_topic(config_json, user_id, service_name, service_id,app_id, place_id)
			collection.update_one({'service_id': service_id}, {'$set' : {'sensor_topics':sensor_topics, 'state':'4'}})
		else:
			sensor_topics = service['sensor_topics']

		print("success till 4")

		output_topic = 'action_service_topic'
		machineName, machinePassword = 'root', 'root'

		if(int(state) < 5):
			print("failed at 5")
			status = SendFullRepo(machineName, machinePassword, container_name, app_id, service_name, service_id)
			collection.update_one({'service_id': service_id}, {'$set' : {'state':'5'}})
		print("success till 5")

		if(int(state) < 6):
			print("failed at 6")
			pid = deployeActual(machineName, machinePassword, container_name, app_id, service_name, service_id, service_file, sensor_topics, output_topic)
			collection.update_one({'service_id': service_id}, {'$set' : {'pid': str(pid), 'state':'6'}})
		else:
			pid = service['pid']

		print("success till 6")