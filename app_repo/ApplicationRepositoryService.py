import os.path
from os import path
from kafka import KafkaProducer , KafkaConsumer
import zipfile
import requests as rq
import json , time
import paramiko
from os import listdir
from flask import Flask, jsonify, request
import threading
import socket

app_repo_ip = 'app_repo'
app_repo_port = 7007
app = Flask(__name__)

#TODO: create requirement.txt by parsing config

def validateJSON(jsonData):
	try:
		json.loads(jsonData)
	except ValueError as err:
		return False
	return True


@app.route('/upload_file', methods=['GET', 'POST'])
def uploadFile():
	req = request.json
	app_name = req['app_name']
	zip_path = "./repository/"+app_name+".zip"
	app_path = "./repository/"
	with zipfile.ZipFile(zip_path, 'r') as zip_ref:
		zip_ref.extractall(app_path)
	
	os.remove(zip_path)

	#check for src folder
	src_path = app_path+"/"+app_name+"/src"
	if not path.exists(src_path):
		return jsonify({"status" : "src folder missing"})

	#check for config file
	config_filepath = app_path+"/"+app_name + "/app_config.json"
	if not path.exists(config_filepath):
		return jsonify({"status" : "app_config.json missing"})

	#validate JSON
	if not validateJSON(config_filepath):
		return jsonify({"status" : "Invalid JSON file"})

	return jsonify({"status" : "ok"})

@app.route('/send_config_file', methods=['GET', 'POST'])
def sendConfigFile():
	req = request.json
	print("got request from deployer")
	app_name = req['app_id']
	file_path = "./repository/"+app_name+'/app_config.json'
	file = open(file_path,'r')
	config_obj = json.load(file)
	print("send config")
	return jsonify(config_obj)

@app.route('/send_files_machine',methods=['GET', 'POST'])
def sendAppToMachine():
	print('hitted send file to machine')
	req = request.json
	machine_name = req['machineName']
	machine_password = req['machinePassword']
	machine_ip = req['machineIp']
	app_id = req['app_id']
	service_name = req['serviceName']
	service_id = req['service_id']
	print('extracted values from deployer')
	
	#ssh to client
	ssh_client = paramiko.SSHClient()
	ssh_client.load_system_host_keys()
	print('client created')
	ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	ssh_client.connect(hostname=machine_ip,username=machine_name,password=machine_password)
	print('connected with ssh')
	
	stdin, stdout, stderr = ssh_client.exec_command("echo root | mkdir '"+service_id+"'")
	stdin, stdout, stderr = ssh_client.exec_command("echo root | chmod 777 '"+service_id+"'")
	ftp_conn = ssh_client.open_sftp()
	files_path = './repository/'+app_id+'/src/'+service_name+'/'

	#copy all code files from given service directory to container
	for files in listdir(files_path):
		ftp_conn.put(files_path+files, './' + service_id+'/'+files)
	
	# ftp_conn.close()
	print('done with ssh')
	return jsonify({"status":"success"})


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

		data = {"module" : "app_repo" , "ts" : current_time , "Status" : 1   }
		producer.send("HeartBeat", data)
		producer.flush()
		time.sleep(3)






# def initiateAppRepo():
# 	app.run(host=socket.gethostbyname(socket.gethostname()), port=app_repo_port, debug=False, threaded=True)


if __name__ == "__main__":
	thread1 = threading.Thread(target = heartBeat)
	thread1.start()
	app.run(host= '0.0.0.0', port=app_repo_port, debug=False)
	# app.run(host=socket.gethostbyname(socket.gethostname()), port=app_repo_port, debug=False)
	# t1 = threading.Thread(target=initiateAppRepo)
	# t1.start()