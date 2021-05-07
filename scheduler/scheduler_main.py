from datetime import datetime, timedelta
from time import sleep
import time
from flask import Flask, jsonify, request
from kafka import KafkaProducer , KafkaConsumer
import threading
import heapq
import json
import requests
import pymongo
import sys

MONGO_SERVER_URL = "mongodb://apurva:user123@cluster0-shard-00-00.p4xv2.mongodb.net:27017,cluster0-shard-00-01.p4xv2.mongodb.net:27017,cluster0-shard-00-02.p4xv2.mongodb.net:27017/IAS_test_1?ssl=true&replicaSet=atlas-auz41v-shard-0&authSource=admin&retryWrites=true&w=majority"
MONGO_DB_NAME = "IAS_test_1"
MONGO_COLLECTION_NAME = "ServiceInformation"

'''
	Start Service Deployment Route : /startdeployment
	End Service Deployment Route : /stopdeployment
'''

POLL_DURATION = 10000000 # can replace it with while True
DEPLOYER_HOSTNAME = sys.argv[1]

WEEK_DAYS = {
	"Monday" : 0,
	"Tuesday" : 1,
	"Wednesday" : 2,
	"Thursday" : 3,
	"Friday" : 4,
	"Saturday" : 5,
	"Sunday" : 6
}


class Scheduler:
	def __init__(self):
		self.client = pymongo.MongoClient(MONGO_SERVER_URL)
		self.database = self.client[MONGO_DB_NAME][MONGO_COLLECTION_NAME]
		self.id = self.database.count_documents({})
		self.scheduling_queue = list()
		self.running_queue = list()
		self.aborted_ids = set()
		if self.id != 0:
			self.restore_state()

	def restore_state(self):
		scheduled_docs = self.database.find({'status':'scheduled'})
		for doc in scheduled_docs:
			data = doc['data']
			_id = int(doc['_id'])
			start_time = data['start_time']
			elem = (start_time, _id, data)
			heapq.heappush(self.scheduling_queue, elem)
			print("restored scheduled request with id : ", _id)

		running_docs = self.database.find({'status':'running'})
		for doc in running_docs:
			data = doc['data']
			_id = int(doc['_id'])
			end_time = data['end_time']
			elem = (end_time, _id, data)
			heapq.heappush(self.running_queue, elem)
			print("restored running request with id : ", _id)


	def poll_scheduling_queue(self):
		curr_time = datetime.now()
		to_be_returned = list()
		while len(self.scheduling_queue) > 0:
			key, _id, request = self.scheduling_queue[0]
			if key <= curr_time:
				to_be_returned.append(request["value"].copy())
				heapq.heappop(self.scheduling_queue)
				if request["repeating"]:
					new_start_time = request["start_time"] + timedelta(days=7) #schedule it to next week
					new_end_time = request["end_time"] + timedelta(days=7) #schedule it to next week
					new_req = request.copy()
					new_req["start_time"] = new_start_time
					new_req["end_time"] = new_end_time
					self.add_request(new_req.copy())
				#add to running queue
				self.add_to_running_queue(request, start_id=_id)
			else:
				break
		return to_be_returned

	def poll_running_queue(self):
		curr_time = datetime.now()
		to_be_returned = list()
		while len(self.running_queue) > 0:
			key, _id, request = self.running_queue[0]
			if key <= curr_time:
				to_be_returned.append(request["value"])
				heapq.heappop(self.running_queue)
			else:
				break

		return to_be_returned

	def add_request_to_database(self, _id, req):
		if req['start_time'] <= datetime.now():
			status = 'running'
		else:
			status = 'scheduled'
		data = {'_id':_id, 'data':req, 'status': status}
		self.database.insert_one(data)
		print("Record Created in Database with id : ", _id) 

	def update_status_in_database(self, _id, new_status):
		self.database.update_one({'_id' : _id}, {'$set' : {'status' : new_status}})
		print("Record Updated in Database with id : ", _id, "to", new_status)

	def add_request(self, request):
		_id = self.id
		self.id += 1
		request["value"]["service_id"] = _id
		thread = threading.Thread(target=self.add_request_to_database, args=(_id, request))
		thread.start()
		# print(request)
		elem = (request["start_time"], _id, request.copy())
		heapq.heappush(self.scheduling_queue, elem)
		print("request added to scheduling queue : ", _id, request["value"]["service_id"])

	def add_to_running_queue(self, request, start_id):
		request["value"]["service_id"] = start_id
		elem = (request["end_time"], start_id, request.copy())
		heapq.heappush(self.running_queue, elem)
		print("request added to running queue : ", start_id, request["value"]["service_id"])

scheduler = None

def poll_scheduler_start(scheduler):
	for _ in range(POLL_DURATION):
		start_req_list = scheduler.poll_scheduling_queue()
		for req in start_req_list:
			print("Start Request :",req['service_id'], req["user_id"], req["app_id"], req["service_name_to_run"])
			new_thread = threading.Thread(target=scheduler.update_status_in_database, args=(int(req['service_id']), 'running'))
			new_thread.start()
			req['service_id'] = str(req['service_id'])
			if DEPLOYER_HOSTNAME != "127.0.0.1":
				requests.post(f"http://{DEPLOYER_HOSTNAME}:5001/startdeployment", json=req)
		sleep(1)

def poll_scheduler_end(scheduler):
	for _ in range(POLL_DURATION):
		end_req_list = scheduler.poll_running_queue()
		for req in end_req_list:
			if req['service_id'] in scheduler.aborted_ids:
				scheduler.aborted_ids.remove(req['service_id'])
				continue
			print("End Request :",req['service_id'], req["user_id"], req["app_id"], req["service_name_to_run"])
			new_thread = threading.Thread(target=scheduler.update_status_in_database, args=(int(req['service_id']), 'terminated'))
			new_thread.start()
			req['service_id'] = str(req['service_id'])
			if DEPLOYER_HOSTNAME != "127.0.0.1":
				requests.post(f"http://{DEPLOYER_HOSTNAME}:5001/stopdeployment", json=req)
		sleep(1)

def change_weekdays_to_indices(days):
	return list(map(lambda x : WEEK_DAYS[x], days))

def manage_scheduled_request(scheduler, schedule, request, appname, algo_name):
	start_times = schedule["time"]["startTimes"]
	durations = schedule["time"]["durations"]
	days = change_weekdays_to_indices(schedule["days"])
	for (start_time, dur) in zip(start_times, durations):
		h, m, s = start_time.split(":")
		h, m, s = int(h), int(m), int(s)
		today = datetime.today()
		start_time_obj = datetime(hour=h, minute=m, second=s,
									year=today.year, month=today.month, day=today.day)
		h, m, s = dur.split(":")
		h, m, s = int(h), int(m), int(s)
		duration = timedelta(hours=h, minutes=m, seconds=s)
		curr_weekday = datetime.today().weekday()
		for day in days:
			if day < curr_weekday:
				start_dtime_obj = start_time_obj + timedelta(days=7+day-curr_weekday)
			elif day > curr_weekday:
				start_dtime_obj = start_time_obj + timedelta(days=day-curr_weekday)
			else:
				if datetime.now() < start_time_obj:
					start_dtime_obj = start_time_obj 
				else:
					start_dtime_obj = start_time_obj + timedelta(days=7)
			end_time_obj = start_dtime_obj + duration
			# request["service_id"] = str(scheduler.id)
			schedule_req = {
				"start_time" : start_dtime_obj,
				"end_time" : end_time_obj,
				"repeating" : True,
				"app_name" : appname,
				"algo_name" : algo_name,
				"value" : request.copy()
			}
			print("Scheduling : ")
			print(f"Start time : {start_dtime_obj}")
			print(f"End time : {end_time_obj}")
			scheduler.add_request(schedule_req.copy())


def manage_immediate_request(scheduler, schedule, request, appname, algo_name, end_time):
	'''
	NOTE : for immediate requests we will still need the end time because start time will be
	immediate. So make sure it's filled in the JSON and it's only a one time schedule.
	NOTE : Length of durations list should be 1.
	'''
	start_time = datetime.now()
	# request["service_id"] = str(scheduler.id)
	if end_time is None:
		h, m, s = schedule["time"]["durations"][0].split(":")
		h, m, s = int(h), int(m), int(s)
		duration = timedelta(hours=h, minutes=m, seconds=s)
		end_time = start_time + duration

	schedule_req = {
		"start_time" : start_time,
		"end_time" : end_time,
		"repeating" : False,
		"app_name" : appname,
		"algo_name" : algo_name,
		"value" : request.copy()
	}
	print("Scheduling : ")
	print(f"Start time : {start_time}")
	print(f"End time : {end_time}")
	scheduler.add_request(schedule_req.copy())

def manage_request(request):
	global scheduler
	for appname in request:
		for algo_name in request[appname]["algorithms"]:
			place_id = request[appname]["algorithms"][algo_name]["place_id"]
			new_request = {
				"user_id" : request[appname]["user_id"],
				"app_id" : request[appname]["application_name"],
				"service_name_to_run" : algo_name,
				"service_id" : None, #service_id
				"place_id" : place_id,
				"action" : request[appname]["algorithms"][algo_name]["action"]
			}
			schedule = request[appname]["algorithms"][algo_name]["schedule"]
			if request[appname]["algorithms"][algo_name]["isScheduled"]:
				manage_scheduled_request(scheduler, schedule, new_request, appname, algo_name)
			else:
				manage_immediate_request(scheduler, schedule, new_request, appname, algo_name, None)
				
def json_deserializer(data):
	return json.dumps(data).decode('utf-8')

def json_serializer(data):
	return json.dumps(data).encode("utf-8")

app = Flask(__name__)

@app.route("/schedule_request", methods=["POST"])
def register_appliction():
	user_req = request.json
	manage_request(user_req)
	return jsonify({"msg" : "ok"})

@app.route("/aborted_service/<int:aborted_service_id>", methods=["GET"])
def handle_aborted_service(aborted_service_id):
	global scheduler
	aborted_service_id = int(aborted_service_id)
	print("GOT ABORT REQUEST FOR SERVICE ID :", aborted_service_id)
	document = scheduler.database.find_one({"_id":aborted_service_id})
	if document is None or document == {}:
		return jsonify({"ERROR":"The given Service id could not be found !"})
	full_data = document["data"]
	to_deployer = full_data["value"] #This is the part that needs to be resent to the deployer
	scheduler.update_status_in_database(aborted_service_id, "ABORTED")
	scheduler.aborted_ids.add(aborted_service_id)

	#Sending a new immediate request to the deployer for re-deployment.
	appname = to_deployer["app_id"]
	algo_name =  to_deployer["service_name_to_run"]
	place_id = to_deployer["place_id"]
	action_info = to_deployer["action"]
	new_request = {
		"user_id" : to_deployer["user_id"],
		"app_id" : appname,
		"service_name_to_run" : algo_name,
		"service_id" : None, #service_id
		"place_id" : place_id,
		"action" : action_info
	}
	'''
	schedule = request[appname]["algorithms"][algo_name]["schedule"]
	if request[appname]["algorithms"][algo_name]["isScheduled"]:
		manage_scheduled_request(scheduler, schedule, new_request, appname, algo_name)
	else:
	'''
	end_time = full_data["end_time"]
	manage_immediate_request(scheduler, None, new_request, appname, algo_name, end_time)
	return jsonify({"msg" : "ok"})


def heartBeat():
	kafka_platform_ip = 'host.docker.internal:9092'
	producer = KafkaProducer(bootstrap_servers=[kafka_platform_ip],value_serializer =json_serializer)
	while True:
		t = time.localtime()
		current_time = int (time.strftime("%H%M%S", t))
		# print(current_time)

		data = {"module" : "scheduler" , "ts" : current_time , "Status" : 1   }
		producer.send("HeartBeat", data)
		producer.flush()
		time.sleep(3)


if __name__ == '__main__':
	thread1 = threading.Thread(target = heartBeat)
	thread1.start()
	scheduler = Scheduler()
	start_poller = threading.Thread(target = poll_scheduler_start, args=(scheduler, ))
	end_poller = threading.Thread(target = poll_scheduler_end, args=(scheduler, ))
	start_poller.start()
	end_poller.start()

	app.run(debug=True, port=13337, host='0.0.0.0')