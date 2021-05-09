import os , sys , time , threading , multiprocessing , json , subprocess , pymongo
import random , socket
from flask import Flask,jsonify , request

dburl = "mongodb://apurva:user123@cluster0-shard-00-00.p4xv2.mongodb.net:27017,cluster0-shard-00-01.p4xv2.mongodb.net:27017,cluster0-shard-00-02.p4xv2.mongodb.net:27017/IAS_test_1?ssl=true&replicaSet=atlas-auz41v-shard-0&authSource=admin&retryWrites=true&w=majority"
db_name = "IAS_test_1"
mapping ={}
collection_names = [ 
	"ServiceInformation" , 
	"deployer_logs",
	"host_log",
	"sensor_manager_logger_current",
	"action_manager_log",
	"user_notifications",
	"controller_notifications"
	]
	
modules_to_bootstrap = [ 
	'app_ui',
	'app_repo' , 
	'scheduler' , 
	'service_lcm',
	'load_balancer' , 
	'deployer', 
	'action_manager' , 
	'sensor_manager' , 
	'worker_node_1' , 
	'worker_node_2' ,
	'monitoring'
	]

def get_ip(module_name):
	cmd = 'sudo docker inspect -f \'{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}\' '+ module_name
	return os.popen(cmd).read().strip()

def start_worker(module_name):
	global dburl    ,   db_name
	myclient = pymongo.MongoClient(dburl)
	mydb = myclient[db_name]
	hostcol = mydb["host_log"]

	build = f'sudo docker build -t service_host ./service_host'
	subprocess.call((build),shell=True)

	cmd = f'sudo docker run -d --network dbz --name {module_name} service_host'
	try : 
		subprocess.call((cmd),shell=True)
		ip = get_ip(module_name)
		print(f" {module_name} running on address {ip}")
		
		item={"name":module_name,"ip":ip}
		hostcol.insert_one(item) 
	except Exception as e: 
		print(f" {module_name} Failed")
		print(str(e))
	return ip

def start_machine(module_name ,ports_mapping = []):

	build = f'sudo docker build -t {module_name} ./{module_name}'
	subprocess.call((build),shell=True)
	ports_str = ''
	for port in ports_mapping:
		ports_str += f' -p {port}:{port}'
	run = f'sudo docker run -d {ports_str} --add-host host.docker.internal:host-gateway --name {module_name} --net=dbz  {module_name}'
	try : 
		subprocess.call((run),shell=True)
		ip = get_ip(module_name)
		print(f" {module_name} running on address {ip}")
	except : 
		print(f" {module_name} Failed on address {ip}")
	return ip

def start_zookeeper(module_name = 'zookeeper'):
	zoo_cmd = "sudo docker run -d --name zookeeper --network=dbz -e ALLOW_ANONYMOUS_LOGIN=yes bitnami/zookeeper"
	try : 

		subprocess.call((zoo_cmd), shell=True)
		ip = get_ip(module_name)
		print(f" {module_name} running on address {ip}")
	except : 
		print(f" {module_name} Failed on address {ip}")
	return ip

def start_kafka(module_name = 'kafka'):
	kafka_cmd = 'sudo docker run -d --name kafka --network=dbz -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 -e ZOOKEEPER_ADVERTISED_HOST_NAME=kafka -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 -e ALLOW_PLAINTEXT_LISTENER=yes -e KAFKA_LISTENERS=PLAINTEXT://:9092 bitnami/kafka'
	try : 
		subprocess.call((kafka_cmd), shell=True)
		ip = get_ip(module_name)
		print(f" {module_name} running on address {ip}")
	except : 
		print(f" {module_name} Failed on address {ip}")
	return ip

def update_ip_json(module_name , ip) :
	global mapping
	mapping[module_name] = ip
	with  open ('ip.json', "w") as f:
		json.dump(mapping , f)

def is_running(module_name):
	cmd = f'sudo docker inspect {module_name} | grep "Running"'
	try :
		ans = subprocess.check_output(cmd , shell = True)
		reply = ans.decode("UTF-8")
	except :
		reply = 'false'

	if 'false' in reply:
		return False
	else :
		return True

app = Flask(__name__)
@app.route('/')
def dummy():
	return "Hello from fault tolerance"

@app.route("/get_platform_modules" , methods=["GET"])
def return_ip_json():
	with  open ('ip.json', "r") as f:
		temp = json.load(f)
	return temp

@app.route("/faulty", methods=["POST"])     #main function
def get_faulty_module():
	req = request.json
	print("Got below request ")
	print(req)
	print()
	module_name = req['module_name']
	if is_running(module_name):
		return jsonify({"Status": f'{module_name} is already running'})
	
	try :
		remove = f'sudo docker rm {module_name}'
		subprocess.call((remove), shell=True)
	except :
		pass

	ip = up_machine(module_name)
	if ip == 'NULL':
		status = "failed"
	else:
		status = f'{module_name} restarted SUCCESSFULLY on address {ip}'
	try :
		update_ip_json(module_name , ip)
	except:
		pass
	return jsonify({"Status": status})


def up_machine(module_name):
	ip = 'NULL'
	if 'worker_node' in module_name:
		ip = start_worker(module_name)
	else :
		ports = []
		if module_name == 'scheduler':
			ports = [13337]
		elif module_name == 'sensor_manager':
			ports = [7071 , 7072 , 5050]
		elif module_name == 'app_ui':
			ports = [9999]
		elif module_name == 'load_balancer':
			ports = [55555]
		elif module_name == 'service_lcm':
			ports = [8089]
		elif module_name == 'deployer':
			ports = [5001]
		elif module_name == 'app_repo':
			ports = [7007]
		ip = start_machine(module_name,ports)
	return ip

def delete_logs():
	global dburl    ,   db_name , collection_names
	client = pymongo.MongoClient(dburl)
	# mydb = myclient[db_name]
   
	for col in collection_names:
		database = client[db_name][col]
		database.delete_many({})
		print(f"Cleared log for {col} database")

def create_new_docker_network(network_name = 'dbz'):
	cmd = f'sudo docker network create {network_name}'
	try:
		subprocess.call((cmd), shell=True)
	except :
		pass


if __name__ == '__main__':
	delete_logs()
	create_new_docker_network()
	zoo_ip = start_zookeeper()
	mapping['zookeeper'] = zoo_ip
	kafka_ip = start_kafka()
	mapping['kafka'] = kafka_ip

	for module_name in modules_to_bootstrap:
		ip = up_machine(module_name)
		mapping[module_name] = ip

	with  open ('ip.json', "w") as f:
		data = json.dump(mapping , f)
	
	app.run(host = '0.0.0.0' , port = 6969)
