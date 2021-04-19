from flask import Flask,jsonify , request
import random, socket , time, json , subprocess , sys , os , requests

def get_ip(module_name):
	cmd = 'docker inspect -f \'{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}\' '+ module_name
	return os.popen(cmd).read().strip()

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


def update_ip_json(module_name , ip) :
	with  open ('ip.json', "r") as f:
		mapping = json.load(f)
	mapping[module_name] = ip
	with  open ('ip.json', "w") as f:
		json.dump(mapping , f)


app = Flask(__name__)
@app.route('/')
def dummy():
	return "Hello from fault tolerance"

@app.route("/get_platform_modules" , methods=["GET"])
def return_ip_json():
	with  open ('ip.json', "r") as f:
		mapping = json.load(f)
	return mapping

@app.route("/faulty", methods=["POST"])
def get_faulty_module():
	req = request.json
	print("got request from heartBeat Manager")
	print(req)
	module_name = req['module_name']

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
	try :		
		ip = start_machine(module_name , ports)
		ports = []

		update_ip_json(module_name , ip)
		status = f'{module_name} restarted SUCCESSFULLY'
	except :
		status = "failed"
	return jsonify({"Status": status})

	
	
if __name__ == "__main__":
	app.run(host = 'localhost' , port = 6969)
	# app.run(host = 'docker.for.win.localhost' , port = 5555)
	# app.run(host = '172.19.0.1' , port = 5555)
	# docker.for.win.localhost
	# app.run(host=socket.gethostbyname(socket.gethostname()),port=5000)