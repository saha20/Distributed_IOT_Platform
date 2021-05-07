from sensor_package import * 
from sm_util import *
import multiprocessing
#not using sensorlogs

collection_name = "sensors_registered"   #sensor_document
general_log_collection_name = "sensor_manager_visual_logs"

stop_state = "stopped"
running_state = "running"

restart_command = "restart"
start_command = "start"
stop_command = "stop"

def general_log(coll,d):
	print("inserting  "+str(d))
	coll.insert_one(d)

proceesses_running = []
app = Flask(__name__)

@app.route('/stopService', methods = ['GET','POST'])
def stopSensorManagerUtil():

	data = request.get_json(force=True)
	serviceid = data['serviceid']
	msg = stop_service(serviceid)
	res = {
		'msg' : msg
	}
	jres = jsonify(res)
	cluster = MongoClient(dburl)
	db = cluster[db_name]
	general_log_collection = db[general_log_collection_name]
	general_log(general_log_collection, res)
	return jres

@app.route('/sensorManagerStartService', methods = ['GET','POST'])
def sensorManagerUtil():

	cluster = MongoClient(dburl)
	db = cluster[db_name]
	general_log_collection = db[general_log_collection_name]

	data = request.get_json(force=True)
	log_res = {
		'Data_received_deployer' : data
	}
	general_log(general_log_collection, log_res)
	# change for place id
	# correctly_parsed_outer_json, userid, applicationName, servicename, serviceid,latitude, longitude, config_file, not_correct_list  = parse_request_sensor_manager(data)
	
	correctly_parsed_outer_json, userid, applicationName, servicename, serviceid, place_id, config_file, not_correct_list  = parse_request_sensor_manager(data)
	
	if(correctly_parsed_outer_json == False):
		msg = "Fields "
		for field in not_correct_list:
			msg = msg + field+ ", "
		msg = msg + " not present in the input received from service node, serviceid : "+serviceid+"."
		res = { 
			'Error' : msg
		}
		jres = jsonify(res)
		general_log(general_log_collection, res)
		return jres


	#list of all sensors used for this service
	correctly_parsed_inner_json, required_sensor_types_list, msg = get_sensor_types_list_in_service(config_file, servicename, applicationName)
	if(correctly_parsed_inner_json == False):
		res = { 
			'Error' : msg
		}
		jres = jsonify(res)
		general_log(general_log_collection, res)
		return jres

	print("required_sensor_types_list : ")
	print(required_sensor_types_list)


	# get sensor topic using the sensor types and location
	# got_nearest_sensors, sensor_topic_id_name_list_for_all_sensors, sensor_instances_not_registered_list = get_sensor_topic(required_sensor_types_list, latitude, longitude)

	# change for place id
	latitude, longitude = get_location_from_place_id(place_id)
	got_nearest_sensors, sensor_topic_id_name_list_for_all_sensors, sensor_instances_not_registered_list = get_sensor_topic(required_sensor_types_list, place_id, latitude, longitude)
	

	if(got_nearest_sensors == False):
		msg = "The sensor instances for sensor types : "
		for s in sensor_instances_not_registered_list:
			msg = msg + s + ", "
		msg = msg + " required by the service : "+servicename+" of application: "+applicationName+" is not registered with the platform. Please register before using."
		res = {
			'Error' : msg
		}
		jres = jsonify(res)
		# res = { 
		# 	'Error' : msg
		# }
		general_log(general_log_collection, res)
		return jres
	# print("sensor_topic_id_name_list_for_all_sensors : ",sensor_topic_id_name_list_for_all_sensors)


	# create temporary topic using serviceid and a random number
	temptopic = serviceid + str(random.randrange(0, 1000))

	# per service process creation
	process = multiprocessing.Process(target = bind_sensor_data_to_temptopic, args=(sensor_topic_id_name_list_for_all_sensors, serviceid, temptopic, applicationName, start_command,))
	process.start()

	print(f'temptopic of serviceid {serviceid} is {temptopic}')
	res = { 
		'temporary_topic' : str(temptopic)
	}
	jres = jsonify(res)
	general_log(general_log_collection, res)
	return jres

if __name__ == '__main__':
	print("Inside sensor_manager")

	# global cluster
	# cluster = MongoClient(dburl)
	# global db 
	# db = cluster[db_name]
	# start listening to the action manager
	main_pid = os.getpid()
	t1 = threading.Thread(target = listen_action_manager, args=())
	t1.start()
	# t2 = threading.Thread(target = listen_exit_command, args=(main_pid,))
	# t2.start()

	# if in case of sudden shutdown, check logs and restart services and topics
	restart_services()

	# run apis

	# app.run(host=socket.gethostbyname(socket.gethostname()),port=manager_port)
	app.run(host='0.0.0.0',port=manager_port)







