import os , json , time , threading , smtplib
from kafka import KafkaProducer , KafkaConsumer
from json import loads, dumps
from pymongo import MongoClient


# collection_name = "sensors_registered"   # sensors_registered_document
logging_collection = "action_manager_log"
controller_collection = "controller_notifications"
user_collection = "user_notifications"
# logging_archive = "logger_archive"

recv_state = "received"
sent_state = "sent"

restart_command = "restart"
start_command = "start"
stop_command = "stop"



# dburl = "mongodb://souptik:admin@cluster0-shard-00-00.dgspa.mongodb.net:27017,cluster0-shard-00-01.dgspa.mongodb.net:27017,cluster0-shard-00-02.dgspa.mongodb.net:27017/IAS_TEST_1?ssl=true&replicaSet=atlas-11r7c8-shard-0&authSource=admin&retryWrites=true&w=majority"
dburl = "mongodb://apurva:user123@cluster0-shard-00-00.p4xv2.mongodb.net:27017,cluster0-shard-00-01.p4xv2.mongodb.net:27017,cluster0-shard-00-02.p4xv2.mongodb.net:27017/IAS_test_1?ssl=true&replicaSet=atlas-auz41v-shard-0&authSource=admin&retryWrites=true&w=majority"
db_name = "IAS_test_1"

kafka_platform_ip = None
sms , email , sms_email = 1 , 2 , 3     


url = "https://www.fast2sms.com/dev/bulk"
  
my_data = {
	'sender_id': 'FSTSMS', 
	'message': '', 
	'language': 'english',
	'route': 'p',
	'numbers': ''    
}
  

headers = {
	'authorization': 'Oad6mcfFgAjsKY9JnHLrGMpqvhy2zQB3okXE0UITVRl4Wb8S7PdO6UsTLwGoVDqW4Ylnpc785MZFAvXg',
	'Content-Type': "application/x-www-form-urlencoded",
	'Cache-Control': "no-cache"
}

def sendSms(receiver , msg):
	# my_data['numbers'] = receiver
	# my_data['message'] = msg
	# response = requests.request("POST",
 #                            url,
 #                            data = my_data,
 #                            headers = headers)
	# # load json data from source
	# returned_msg = json.loads(response.text)
	# print(returned_msg['message'])
	print("received msg : ", msg)
	cluster = MongoClient(dburl)
	db = cluster[db_name]
	collection = db[user_collection]

	logging_entry = {
			"notify_user" : receiver,
			"user_display" : msg
		}
	collection.insert_one(logging_entry)
	print("inserted in db")



def sendMail(receiver , msg  ):
	print("inside send mail")
	print(msg)
	cluster = MongoClient(dburl)
	db = cluster[db_name]
	collection = db[user_collection]

	logging_entry = {
			"notify_user" : receiver,
			"user_display" : msg
		}
	collection.insert_one(logging_entry)
	print("inserted in db")

	sender = 'ias.platform.app@gmail.com'
	password = 'Ias@Platfrom'
	smtpserver = smtplib.SMTP("smtp.gmail.com",587)
	smtpserver.starttls()
	smtpserver.login(sender,password)
	
	smtpserver.sendmail(sender,receiver,msg)
	print('sent')
	smtpserver.close()

def getKafkaIP():
	# with  open ('ip_port.json', "r") as f:
	#     data = json.load(f)
	# kafka_platform_ip = data['allocatedPool']['Kafka']
	kafka_platform_ip = 'host.docker.insternal:9092'
	return kafka_platform_ip


def json_deserializer(data):
	return json.dumps(data).decode('utf-8')

def json_serializer(data):
	return json.dumps(data).encode("utf-8")


def service_count(service_id, collection):
	count = 0
	for x in collection.find():
		if (x['service_id'] == service_id):
			count=count+1 
	return count


def doLogging(service_id, to_sensor_manager, to_notify_users, to_user_display, state) :
	cluster = MongoClient(dburl)
	db = cluster[db_name]
	collection = db[logging_collection]
	service_id = str(service_id)

	if state == start_command :
		logging_entry = {
			"service_id" : service_id,
			"state" : recv_state,
			"to_sensor_manager" : to_sensor_manager,
			"to_notify_users" : to_notify_users,
			"to_user_display" : to_user_display
		}

		count = service_count(service_id, collection)

		if (count>0):
			print(" From doLogging() inside **action_manager** ")
			msg = "Service with service_id "+service_id+" was already running, closing previous and restarting this."
			# do_logging(serviceid, restart_command, applicationname, temptopic, sensor_topic_id_name_list_for_all_sensors, process_id)
			# return msg
		else:
			collection.insert_one(logging_entry)
			print("inserted in db")
	elif state == stop_command :
		count = service_count(service_id, collection)

		if(count!=1): 
			print("error in service_id received")
			# if no service was found
			if(count==0):
				print("No service with service_id "+service_id+" present.")
			
			else :
				print("Multiple occurences of service with service_id "+service_id+" found!")

		for x in collection.find():
			print(x)
			if ( (x["service_id"]) == service_id ):
				myquery = { "service_id": service_id }
				new_values = { "$set": { "state": sent_state } }
				collection.update_one(myquery, new_values)
				print("updated in db")



def notifySensorManager(service_id, to_sensor_manager, to_notify_users, to_user_display, state):
	cluster = MongoClient(dburl)
	db = cluster[db_name]
	collection = db[controller_collection]

	# producer for SM :
	kafka_platform_ip = getKafkaIP()

	producer = KafkaProducer(bootstrap_servers=[kafka_platform_ip],value_serializer =json_serializer)
	# data = {"sensor_manager" : data_SM}
	count = 0
	temp_list = list()
	for data in to_sensor_manager:
		temp_list.append(data)
		producer.send("actionManager_to_sensorManager", data)
		producer.flush()
		count += 1 
		if count == 2 :
			logging_entry = {
				"sensor_id" : temp_list[0]["sensor_id"],
				"command" : temp_list[1]["command"]
			}
			collection.insert_one(logging_entry)
			print("inserted in db")
			count = 0
			temp_list.clear()


	# "sensor_id" : "7276753169",
	# "command" : "Display this command"        
	# need to log after sending command to SM
	doLogging(service_id, to_sensor_manager, to_notify_users, to_user_display, state)


def notifyUsers(service_id, to_sensor_manager, to_notify_users, to_user_display, state):
	sz = len(to_notify_users)

	for user_addr in to_notify_users:
		if '@' in user_addr:
			sendMail(user_addr, "hi... this is for testing")
		else:
			sendSms(user_addr , "demo msg")
	# need to log after sending notification
	# doLogging(service_id, to_sensor_manager, to_notify_users, to_user_display, state)            



def restartServices():
	cluster = MongoClient(dburl)
	db = cluster[db_name]
	collection = db[logging_collection]
	d = list(collection.find())
	if(len(d) != 0):
		for i in d:
			state = i['state']
			if(state == recv_state):
				service_id = i['service_id']
				to_sensor_manager = i['to_sensor_manager']
				to_notify_users = i['to_notify_users']
				to_user_display = i['to_sensor_manager']
				
				notifySensorManager(service_id, to_sensor_manager, to_notify_users, to_user_display, state='stop')
				notifyUsers(service_id, to_sensor_manager, to_notify_users, to_user_display, state='stop')
				time.sleep(3)
				print(f'started a service from logs {service_id}')



def listenForInstruction():
	# check restart
	restartServices()
	print("hello from AM")
	# listen_data = consume
	consumer = KafkaConsumer('action_manager',
	 bootstrap_servers = ['host.docker.internal:9092'],
	 auto_offset_reset = 'earliest',
	 enable_auto_commit = True,
	 group_id = 'my-group',
	 value_deserializer=lambda x: loads(x.decode('utf-8')))
	 # value_deserializer = json_deserializer)
	
	for message in consumer:
		message = message.value
		# collection.insert_one(message)
		# print('{} added to {}'.format(message, collection))
		print("Printing from listenForInstruction")
		print(message)
		listen_data = message["action_center"]
		service_id = listen_data["service_id"]
		to_sensor_manager = listen_data["sensor_manager"]
		to_notify_users = listen_data["notify_users"]
		to_user_display = listen_data["user_display"]
		subject = to_user_display

		# need to log the received instructions
		doLogging(service_id, to_sensor_manager, to_notify_users, to_user_display, state='start')


		if len(to_sensor_manager) > 0:
			notifySensorManager(service_id, to_sensor_manager, to_notify_users, to_user_display, state='stop')

		notifyUsers(service_id, to_sensor_manager, to_notify_users, to_user_display, state='stop')
		time.sleep(10)
	


def heartBeat():
	kafka_platform_ip = getKafkaIP()
	producer = KafkaProducer(bootstrap_servers=[kafka_platform_ip],value_serializer =json_serializer)
	while True:
		t = time.localtime()
		current_time = int (time.strftime("%H%M%S", t))
		# print(current_time)

		data = {"module" : "action_manager" , "ts" : current_time , "Status" : 1   }
		producer.send("HeartBeat", data)
		producer.flush()
		time.sleep(3)



if __name__ == "__main__":
	thread1 = threading.Thread(target = heartBeat)
	# listenForInstruction()
	thread1.start()

	listenForInstruction()

