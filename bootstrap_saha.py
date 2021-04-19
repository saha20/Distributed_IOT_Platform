import os , sys , time , threading , multiprocessing , json , subprocess , paramiko

mapping ={}
# modules_to_bootstrap = [ 'load_balancer' , 'fault_tolerance' , 'request_manager' , 'scheduler' ,'worker_node_1', 'deployer' , 'action_manager' ,'sensor_manager' , 'heart_beat']
modules_to_bootstrap = [ 'app_repo' , 'load_balancer' , 'deployer', 'scheduler' , 'action_manager' , 'sensor_manager' , 'monitoring']
def get_ip(module_name):
	cmd = 'docker inspect -f \'{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}\' '+ module_name
	return os.popen(cmd).read().strip()

def start_worker(module_name):

	build = f'docker build -t service_host ./service_host'
	os.system(build)
	cmd = f'docker run -dit  --network platform_net --name {module_name} service_host'
	try : 
		os.system(cmd)
		ip = get_ip(module_name)
		print(f" {module_name} running")
		ssh_client=paramiko.SSHClient()
		ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		ssh_client.connect(hostname = ip , port = 22 , username = 'root' , password = 'root')
		print(f" {module_name} ssh running")
		transport = ssh_client.get_transport()
		channel = transport.open_session()	
		pty = channel.get_pty()
		shell = ssh_client.invoke_shell()
		shell.send("cd ../work_dir/; nohup python3 container_server.py > /dev/null 2>&1 &\n")
		time.sleep(2)
		
		
	except Exception as e: 
		print(f" {module_name} Failed")
		print(str(e))

def start_machine(module_name ,ports_mapping = []):

	build = f'docker build -t {module_name} ./{module_name}'
	subprocess.call((build))
	ports_str = ''
	for port in ports_mapping:
		ports_str += f' -p {port}:{port}'

	run = f'docker run -d {ports_str} --name {module_name} --net=dbz  {module_name}'
	try : 
		subprocess.call((run))
		ip = get_ip(module_name)
		print(f" {module_name} running on address {ip}")
	except : 
		print(f" {module_name} Failed on address {ip}")
	return ip

def start_zookeeper(module_name = 'zookeeper'):
	zoo_cmd = 'docker run -d --name zookeeper --network=dbz -e ALLOW_ANONYMOUS_LOGIN=yes bitnami/zookeeper'
	try : 
		# subprocess.Popen(zoo_cmd)
		# os.system(zoo_cmd)
		subprocess.call((zoo_cmd))
		ip = get_ip(module_name)

		print(f" {module_name} running on address {ip}")
	except : 
		print(f" {module_name} Failed on address {ip}")
	return ip

def start_kafka(module_name = 'kafka'):
	kafka_cmd = 'docker run -d --name kafka --network=dbz -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 -e ZOOKEEPER_ADVERTISED_HOST_NAME=kafka -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 -e ALLOW_PLAINTEXT_LISTENER=yes -e KAFKA_LISTENERS=PLAINTEXT://:9092 bitnami/kafka'
	# kafka_cmd = 'docker run -d --name kafka --network=dbz -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 -e ZOOKEEPER_ADVERTISED_HOST_NAME=kafka -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092 -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT -e KAFKA_INTER_BROKER_LISTENER_NAME:PLAINTEXT -e ALLOW_PLAINTEXT_LISTENER=yes -e KAFKA_LISTENERS=PLAINTEXT://:9092,PLAINTEXT_HOST://localhost:29092 bitnami/kafka'
	
	try : 
		# subprocess.Popen(kafka_cmd)
		# os.system(kafka_cmd)
		subprocess.call((kafka_cmd))
		ip = get_ip(module_name)

		print(f" {module_name} running on address {ip}")
	except : 
		print(f" {module_name} Failed on address {ip}")
	return ip

#from stackoverflow
# KAFKA_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
#   KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
#   KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
#   KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092


if __name__ == '__main__':

	zoo_ip = start_zookeeper()
	mapping['zookeeper'] = zoo_ip

	kafka_ip = start_kafka()
	mapping['kafka'] = kafka_ip

	for module_name in modules_to_bootstrap:
		ip = 'NULL'
		if 'worker_node' in module_name:
			ip = start_worker(module_name)
		else :
			ports = []
			if module_name == 'scheduler':
				ports = [13337]
			elif module_name == 'sensor_manager':
				ports = [7071 , 7072 , 5050]
			elif module_name == 'load_balancer':
				ports = [55555]
			elif module_name == 'deployer':
				ports = [5001]
			elif module_name == 'app_repo':
				ports = [7007]
			ip = start_machine(module_name,ports)
		mapping[module_name] = ip

	with  open ('ip.json', "w") as f:
		data = json.dump(mapping , f)


	#  start zookeepr 
	#  start kafka
	#  start other modules