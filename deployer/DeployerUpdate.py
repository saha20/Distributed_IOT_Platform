import threading
from flask import Flask, jsonify, request
import json
import requests as rq
import time as t
# import pandas as pd
import json
from pathlib import Path
# from kafka import KafkaConsumer, KafkaProducer
import os
import paramiko

#appRep 
appRepo_url = 'http://app_repo:7007'
#load balancer ip port 
lb_url = 'http://load_balancer:55555'
#sensor manager url
sm_url = "http://sensor_manager:5012/sensorManagerStartService"

#kafka_ip
KAFKA_IP = 'kafka:9092'

def containerDelete(container_details,service_id):
	container_details = container_details.split('|')
	container_id, machineName, machinePassword, node_ip, node_port = container_details[0],container_details[1],container_details[2],container_details[3],container_details[4]

	ssh_client = paramiko.SSHClient()
	ssh_client.load_system_host_keys()
	ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	ssh_client.connect(hostname=node_ip, username=machineName,password=machinePassword)
	#TODO: remove service pid fron=m container n keep a map for same
	# ssh_client.exec_command('echo root | sudo docker container rm -f '+container_id)
	# ssh_client.exec_command('echo root | sudo rm -f '+ " '"+service_id+"'")
	# os.system('sudo docker container rm -f '+container_id)
	# os.system('sudo docker container rm -f '+container_id)


def deployeActual(machineName, machinePassword, node_ip, app_id, service_name, service_id, service_file,temp_topic,output_topic):
	ssh_client = paramiko.SSHClient()
	ssh_client.load_system_host_keys()
	ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	ssh_client.connect(hostname=node_ip, username=machineName,password=machinePassword)

	
	#run service script file
	filepath = './'+service_id+'/'+service_file
	print("running file", filepath)
	stdin,stdout,stderr=ssh_client.exec_command('echo root | python3 -u '+filepath)
	print("script running succesffully")

	# return stdout.readlines()[0]



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



def get_sensor_topic(config_json, user_name, service_name,service_id,app_id, longitude, latitude):
	application_name = config_json[app_id]['application_id']
	sensor_topics = []
	lat = str()
	longitude = str()
	
	
	
	req = {
		'username' : user_name,
		'applicationname' : application_name,
		'servicename' : service_name,
		'serviceid' : service_id,
		'latitude' : latitude,
		'longitude' : longitude,
		'config_file' : config_json
	}
	res = rq.post(url = sm_url, json = req)
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
	latitude = req['latitude']
	longitude = req['longitude']
	
	print("returning from req handler")
	return service_name, user_id, app_id, action_details, service_id, longitude, latitude

def freeServer(req):
	status = rq.post(lb_url+'/free_server', json = req)

def requestLoadBalancer(user_id, app_id, service_name, service_id):

	#request ip port
	print("\n\b in request Load balancer")

	lb_response = rq.post(lb_url+'/return_host', json = "")
	lb_response = lb_response.json()
	node_ip = lb_response['ip']
	# node_port = lb_response['port']
	print("Got Load balancer ip port")

	#log service to load balancer 
	req = {}
	req['app_id'] = app_id
	req['user_id'] = user_id
	req['service_name_to_run'] = service_name
	req['service_id'] = service_id
	rsp = rq.post(lb_url+'/log_application', json = req)
	print("\n\b sent request in load balancer")
	return node_ip, node_port

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

