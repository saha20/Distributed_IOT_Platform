from sensor_package import * 
import multiprocessing

collection_name = "sensors_registered"   # sensors_registered_document
logging_collection = "logger_current"
logging_archive = "logger_archive"

stop_state = "stopped"
running_state = "running"

restart_command = "restart"
start_command = "start"
stop_command = "stop"

proceesses_running = []

'''
Below are the keys in the json that we get to serve a service
'''
app_config_username = "username"
app_config_applicationname = "applicationname"
app_config_servicename = "servicename"
app_config_serviceid = "serviceid"
app_config_latitude = "latitude"
app_config_longitude = "longitude"
app_config_config_file = "config_file"
app_config_algorithms = "algorithms"
app_config_sensors = "sensors"

'''
Below are the keys in the json that we get to serve a service
'''
# sensor_instance_config_ = "xxx"
# sensor_instance_config_ = "xxx"
# sensor_instance_config_ = "xxx"
# sensor_instance_config_ = "xxx"
# sensor_instance_config_ = "xxx"
# sensor_instance_config_ = "xxx"
# sensor_instance_config_ = "xxx"

app = Flask(__name__)

def check_field_in_json(data,field):
	if(field in data): 
		return True
	else: 
		return False

def service_count(serviceid,collection):
	count = 0
	for x in collection.find():
		if (x['serviceid'] == serviceid):
			count=count+1 
	return count

def close_process(pid):
	pid = str(pid)
	to_kill = True
	msg = "Process killed with pid "+pid
	try:
		os.kill(int(pid), 0)
	except OSError:
		to_kill = False
		msg = "No process with pid "+pid
	if(to_kill) : 
		command = "kill -9 "+pid
		os.system(command)
	return msg

def do_logging(serviceid, state=stop_command, applicationname=None, temptopic=None, sensor_topic_id_name_list_for_all_sensors=None, process_id=None):
	msg = "Service with serviceid "+serviceid+" is running."
	cluster = MongoClient(dburl)
	db = cluster[db_name]
	collection = db[logging_collection]
	serviceid = str(serviceid)
	# stopping a service
	if(state==stop_command):

		count = service_count(serviceid, collection)
		msg = "Service with serviceid "+serviceid+" is stopped being served."

		if(count!=1): 
			print("Zhol in serviceid received")
			# if no service was found
			if(count==0):
				msg = "No service with serviceid "+serviceid+" present."
				return msg
			# if multiple services were found, stop them all, change their start to stopped
			else :
				msg = "Multiple occurences of service with serviceid "+serviceid+" found!, closing all."

		for x in collection.find():
			print(x)
			if ( (x["serviceid"]) == serviceid ):
				temptopic = x["temptopic"]
				process_id = x["process_id"]
				print("Closing the temporary topic : ", temptopic)
				print(close_process(process_id))
				myquery = { "serviceid": serviceid }
				newvalues = { "$set": { "state": stop_state } }
				collection.update_one(myquery, newvalues)

	# while startring  a service
	elif(state==start_command):

		logging_entry = {
			"serviceid" : serviceid,
			"state" : running_state,
			"applicationname" : applicationname,
			"temptopic" : temptopic,
			"sensor_topic_id_name_list_for_all_sensors" : sensor_topic_id_name_list_for_all_sensors,
			"process_id" : process_id
		}

		count = service_count(serviceid,collection)

		if (count>0):
			print("Kya re divyansh, do service same name? ")
			msg = "Service with serviceid "+serviceid+" was already running, closing previous and restarting this."
			do_logging(serviceid, restart_command, applicationname, temptopic, sensor_topic_id_name_list_for_all_sensors, process_id)
			return msg
		else:
			collection.insert_one(logging_entry)

	else: # restart_command

		logging_entry = {
			"serviceid" : serviceid,
			"state" : running_state,
			"applicationname" : applicationname,
			"temptopic" : temptopic,
			"sensor_topic_id_name_list_for_all_sensors" : sensor_topic_id_name_list_for_all_sensors,
			"process_id" : process_id
		}

		# 13 April change
		# close_process(get_previous_process_id(serviceid, collection)) 
		do_logging(serviceid, stop_command)
		myquery = { "serviceid" : serviceid }
		newvalues = { "$set": logging_entry }
		collection.update_one(myquery, newvalues)

	return msg

def restart_services():
	cluster = MongoClient(dburl)
	db = cluster[db_name]
	collection = db[logging_collection]
	d = list(collection.find())
	if(len(d) != 0):
		for i in d:
			state = i['state']
			if(state == running_state):
				sensor_topic_id_name_list_for_all_sensors = i['sensor_topic_id_name_list_for_all_sensors']
				applicationName = i['applicationname']
				serviceid = i['serviceid']
				temptopic = i['temptopic']
				process = multiprocessing.Process(target = bind_sensor_data_to_temptopic, args=(sensor_topic_id_name_list_for_all_sensors, serviceid, temptopic, applicationName, restart_command))
				process.start()
				print(f'started a service from logs {serviceid}')

def stop_service(serviceid):
	return do_logging(serviceid, stop_command)

def clear_logs():
	cluster = MongoClient(dburl)
	db = cluster[db_name]
	collection = db[logging_collection]
	collection.drop()
	return

def get_sensor_types_list_in_service(d, servicename, applicationName):
	not_correct_list = []
	if(check_field_in_json(d,applicationName)==False):
		return False, None, "Application named "+applicationName+" not present in the config_file."

	if(check_field_in_json(d[applicationName],app_config_algorithms)==False):
		return False, None, "Field  "+app_config_algorithms+" not present in the config_file."

	list_of_services = d[applicationName][app_config_algorithms].keys() #list of services
	list_of_sensors_with_key = []
	required_sensor_types_list = []

	found_service_required_in_config = False
	for service in list_of_services:
		if service == servicename:
			found_service_required_in_config = True
			list_of_sensors_with_key = d[applicationName][app_config_algorithms][service][app_config_sensors]
			break

	if(found_service_required_in_config == False):
		return False, None, "Service named "+servicename+" not present in application "+applicationName+" in the config_file."

	sensor_keys = list_of_sensors_with_key.keys()
	for key in sensor_keys:
		required_sensor_types_list.append(list_of_sensors_with_key[key])

	return True, required_sensor_types_list, "OK"

def get_sensor_topic(query, latitude, longitude):
	cluster = MongoClient(dburl)
	db = cluster[db_name]
	col = db[collection_name]
	sensor_topic_id_name_list = []
	sensor_instances_not_registered_list = []
	got_nearest_sensors = True
	for sensor_num in range(len(query)):
		docs = col.find({})
		min_dist = sys.maxsize
		sensor_topic_id_name = None
		for i in docs:
			if i['sensor_name'] == query[sensor_num]:
				temp_lat = int(i['sensor_geolocation']['lat'])
				temp_long = int(i['sensor_geolocation']['long'])
				latitude = int(latitude)
				longitude = int(longitude)
				if (abs(temp_lat - latitude)+abs(temp_long - longitude)) < min_dist:
					min_dist = abs(temp_lat - latitude) + abs(temp_long-longitude)
					sensor_topic_id_name = i['sensor_data_storage_details']['kafka']['broker_ip'] + "#" + i['sensor_data_storage_details']['kafka']['topic'] + "#" + i['sensor_id'] + "#" + i['sensor_name']
					print("sensor_topic_id_name : ",sensor_topic_id_name)

		if(sensor_topic_id_name == None):
			got_nearest_sensors = False
			sensor_instances_not_registered_list.append(query[sensor_num])
		sensor_topic_id_name_list.append(sensor_topic_id_name)

	return got_nearest_sensors, sensor_topic_id_name_list, sensor_instances_not_registered_list

'''
Step1: consumer reads/consumes all the data from the topic->topic (where sensors have been dumping the data).
Then we iterate through the consumer to get all the msgs that the consumer has read, and send those messages, as producer, to topic->temptopic. From this temptopic, the applications/etc can get the sensor data via sensor mgr
'''

def dump_data(ip, topic, serviceid, temptopic, sensor_id, sensor_name):
	data_rate = 1
	# 13 April change
	print("in dump data")
	print("from the topic :",topic)
	group_id_temp_topic = smgid + str(temptopic)
	consumer = KafkaConsumer('sensor_topic_15', bootstrap_servers=[KAFKA_PLATFORM_IP], auto_offset_reset = "latest",group_id=group_id_temp_topic )
	producer = KafkaProducer(bootstrap_servers=[KAFKA_PLATFORM_IP],value_serializer=json_serializer)
	for message in consumer:
		value = message.value.decode('utf-8')
		msg = sensor_id + "#" + sensor_name + "#" + value
		print("msg ",msg)
		producer.send(temptopic, msg)
		producer.flush()
		time.sleep(int(data_rate))

def listen_action_manager():
	consumer = KafkaConsumer(str(action_manager_topic), bootstrap_servers=[KAFKA_PLATFORM_IP], auto_offset_reset = "earliest",group_id=smgid)
	for message in consumer:
		print("Take below Action : ")
		value = message.value.decode('utf-8')
		print(value)

def bind_sensor_data_to_temptopic(sensor_topic_id_name_list_for_all_sensors, serviceid, temptopic, applicationName, state):
	# add processes to the current running list
	pid = os.getpid()
	proceesses_running.append(pid)
	msg = do_logging(serviceid, state, applicationName, temptopic, sensor_topic_id_name_list_for_all_sensors, pid)
	print("logging done : ",msg)

	####################################################
	print("in bind.")
	consumer = KafkaConsumer(bootstrap_servers=[KAFKA_PLATFORM_IP], auto_offset_reset="latest")
	list_of_topics = []
	list_of_sensor_names = []
	list_of_sensor_ids = []
	dict_index_topic = {}

	for i in range(len(sensor_topic_id_name_list_for_all_sensors)):
		ip, topic, sensor_id, sensor_name = sensor_topic_id_name_list_for_all_sensors[i].split('#')
		# print("ip and topic are :")
		# print(ip+" "+topic)
		list_of_topics.append(topic)
		list_of_sensor_names.append(sensor_name)
		list_of_sensor_ids.append(sensor_id)
		dict_index_topic[topic] = i

	consumer.subscribe(list_of_topics)
	consumer.poll()
	print("after poll.")
	producer = KafkaProducer(bootstrap_servers=[KAFKA_PLATFORM_IP],value_serializer=json_serializer)

	for message in consumer:
		print("message along with sensor id : ")
		value = message.value.decode('utf-8')
		topic = message.topic
		index = dict_index_topic[topic]
		msg = list_of_sensor_ids[index]+"#"+list_of_sensor_names[index]+"#"+value
		print(msg)
		producer.send(temptopic, msg)
		producer.flush()
		time.sleep(int(1))

	# for i in range(len(sensor_topic_id_name_list_for_all_sensors)):
	# 	ip, topic, sensor_id, sensor_name = sensor_topic_id_name_list_for_all_sensors[i].split('#')
	# 	t = threading.Thread(target=dump_data, args=(ip, topic, serviceid, temptopic, sensor_id, sensor_name,))
	# 	t.start()

def listen_exit_command(main_pid):
	inp = input()
	if(inp=="exit()"):
		clear_logs()
		for p in proceesses_running:
			close_process(p)
		# add_to_archive()
		print("Cleared logs.")
		close_process(main_pid)

def parse_request_sensor_manager(data):
	# data_dict = json.loads(data)
	data_dict = data
	not_correct_list = []
	values = []
	correctly_parsed_outer_json = True
	app_config_field_list = [app_config_username, app_config_applicationname, app_config_servicename, app_config_serviceid,  app_config_config_file]
	
	for field in app_config_field_list:
		if(check_field_in_json(data_dict,field)):
			values.append(data[field])
		else:
			not_correct_list.append(field)
			values.append(None)
			correctly_parsed_outer_json = False

	return correctly_parsed_outer_json, values[0], values[1], values[2], values[3], values[4], not_correct_list


