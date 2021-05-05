# sensor_catalogue_registration.py

from sensor_package import * 

app = Flask(__name__)
appport = instance_reg_port
collection_name = sensor_instance_collection
cluster = None
db = None

def get_already_present_place_ids():

	# cluster = MongoClient(dburl)
	# db = cluster[db_name]
	collection = db[place_details_collection]
	result = collection_to_json(collection)
	result = json.loads(result)
	places_present_list = []

	print("place_ids present :")
	for row in result :
		print(row)
		places_present_list.append(row['place_id'])

	return places_present_list

def insert_place_details(d):
	
	# cluster = MongoClient(dburl)
	# db = cluster[db_name]
	collection = db[place_details_collection]
	place_id = "None"
	lat = "None"
	longi = "None"
	places_present_list = get_already_present_place_ids()
	for i in d: # loop for each sensor
		place_id = "None"
		lat = "None"
		longi = "None"
		for j in d[i]:
			if(j=='place_id'):
				place_id = d[i][j]
			if(j=='sensor_geolocation'):
				lat = d[i][j]['lat']
				longi = d[i][j]['long']
		config_new = {}
		config_new['place_id'] = place_id
		config_new['lat'] = lat
		config_new['long'] = longi
		if(place_id in places_present_list):
			continue;
		collection.insert_one(config_new)
		places_present_list.append(place_id)

	
def insert_sensor_topic_details(d):
	
	# cluster = MongoClient(dburl)
	# db = cluster[db_name]
	collection = db[sensor_topic_details_collection]

	for i in d: # loop for each sensor
		sensor_topic_name = "None"
		sensor_type = "None"
		place_id = "None"
		for j in d[i]:
			if(j=='sensor_name'):
				sensor_type = d[i][j]
			if(j=='place_id'):
				place_id = d[i][j]
			if(j=='sensor_data_storage_details'):
				sensor_topic_name = d[i][j]['kafka']['topic']
		config_new = {}
		config_new['sensor_topic_name'] = sensor_topic_name
		config_new['sensor_type'] = sensor_type
		config_new['place_id'] = place_id
		collection.insert_one(config_new)

	# collection.insert_one()

def create_random_topic_name():

	num = random.randint(0,600000)
	name = "sensor_topic_"+str(num)
	return name


def get_already_present_topics():

	# cluster = MongoClient(dburl)
	# db = cluster[db_name]
	collection = db[sensor_topic_details_collection]
	result = collection_to_json(collection)
	result = json.loads(result)
	topics_present_list = []

	print("topics present :")
	for row in result :
		print(row)
		topics_present_list.append(row['sensor_topic_name'])

	return topics_present_list


def get_all_sensor_types():

	cat_coll = sensor_catalogue_collection
	# cluster = MongoClient(dburl)
	# db = cluster[db_name]
	collection = db[cat_coll]
	result = collection_to_json(collection)
	result = json.loads(result)
	type_list = []

	for row in result:
		# print(row)
		type_list.append(row["sensor_name"])

	return type_list

def validate_if_type_present(config, sensor_type_list):

	s_type = config["sensor_name"]
	if(s_type not in sensor_type_list) :
		return True, s_type
	else :
		return False, s_type

def remove_unnecessary_data(d):
	for i in d:
		if(d[i]['sensor_geolocation']['lat'] == "None" or d[i]['sensor_geolocation']['long'] == "None"):
			del d[i]['sensor_geolocation']
	already_present_topics_list = get_already_present_topics()
	for i in d:
		for j in d[i]:
			if(j=='sensor_data_storage_details'):
				if(d[i][j]['kafka']['broker_ip'] == "None"):
					d[i][j]['kafka']['broker_ip'] = "localhost:9092"
				if(d[i][j]['mongo_db']['ip'] == "None"):
					del d[i][j]['mongo_db']
				if(d[i][j]['kafka']['topic'] == "None"):
					d[i][j]['kafka']['topic'] = create_random_topic_name()
					print("Topic was not present in "+i+" created random topic.")

				sensor_topic = d[i][j]['kafka']['topic']

				while(True):
					if sensor_topic in already_present_topics_list:
						print("Topic "+sensor_topic+" was already resgitered, finding new")
						sensor_topic = create_random_topic_name()
					else:
						break

				d[i][j]['kafka']['topic'] = sensor_topic


	return d


@app.route('/getAllSensors',methods=['GET','POST'])
def fun1():

	# cluster = MongoClient(dburl)
	# db = cluster[db_name]
	collection = db[collection_name]
	result = collection_to_json(collection)
	return result

@app.route('/getPlaceIds',methods=['GET','POST'])
def fun0():

	# cluster = MongoClient(dburl)
	# db = cluster[db_name]
	collection = db[place_details_collection]
	result = collection_to_json(collection)
	return result

@app.route('/sensorRegistration',methods=['GET','POST'])
def fun2():

	NoneType = type(None)
	incoming_data = request.get_json(force=True)
	if(type(incoming_data) == NoneType):
		msg={'msg':'No config received'}
		return msg
	user_id = incoming_data["user_id"]
	config = incoming_data["sensor_reg_config"]
	config = remove_unnecessary_data(config)

	# added for new demo 
	insert_place_details(config)
	insert_sensor_topic_details(config)

	present_sensor_types = get_all_sensor_types()
	print(present_sensor_types)

	# cluster = MongoClient(dburl)
	# db = cluster[db_name]
	collection = db[collection_name]

	sensor_type_not_registered = ""
	error_flag = False

	for s in config :
		error, sensor_type = validate_if_type_present(config[s],present_sensor_types)
		if(error == False):
			unique_id = str(uuid4())
			print("unique_id :", unique_id)
			config[s]["sensor_id"] = unique_id    # change for sensor_id inclusion
			config[s]["user_id"] = user_id
			collection.insert_one(config[s])
		else:
			sensor_type_not_registered = sensor_type +", "+sensor_type_not_registered
			error_flag = True

	msg = "sensors registered"
	if error_flag:
		msg = sensor_type_not_registered + " are the sensors types not registered. Please register before use."

	resmsg = {'msg': msg}
	return resmsg
  

if __name__ == '__main__':
	print("Inside sensor_instance_registration")
	cluster = MongoClient(dburl)
	db = cluster[db_name]
	# app.run(host=socket.gethostbyname(socket.gethostname()),port=appport)
	app.run(host="0.0.0.0",port=appport)
