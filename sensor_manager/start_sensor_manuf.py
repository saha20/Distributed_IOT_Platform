"""
- Returning lat long as str (see in create_fake_sensor_data())
- format of data being faked+returned is diff for different sensor types. Needs to be read differently in sensor_mgr too.
"""

from sensor_package import *


topics_collection = sensor_topic_details_collection

fake = Faker()

def create_fake_sensor_data(sensor_type, place_id):
	if (sensor_type == 'hook_rotation'):
		return random.randrange(1, 3000)

	elif (sensor_type == 'thread_remaining'): 
		return random.randrange(5, 2000)

	elif(sensor_type == 'stitch_setting'):
		return random.randrange(1, 5000)

	else:
		return random.randrange(600, 700)


'''
"sensor_type_4" :{
			"sensor_name": "temperature_sensors",
			"sensor_data_type": "int",
			"has_controller" : "yes"
		},

		"sensor_type_5" :{
			"sensor_name": "color_sensor",
			"sensor_data_type": "int",
			"has_controller" : "yes"
		},

		"sensor_type_6" :{
			"sensor_name": "camera_sensor",
			"sensor_data_type": "int",
			"has_controller" : "yes"
		}

'''
		

if __name__ == '__main__':
	
	cluster = MongoClient(dburl)
	db = cluster[db_name]
	collection = db[topics_collection]

	producer = KafkaProducer(bootstrap_servers=[KAFKA_PLATFORM_IP],value_serializer=json_serializer)

	while 1:
		for x in collection.find({}):
			sensor_topic_name = x['sensor_topic_name']
			sensor_type = x['sensor_type']
			place_id = x['place_id']

			data = create_fake_sensor_data(sensor_type, place_id)
			producer.send(sensor_topic_name, data)  # produces data on the topic named sensor_topic_name
			producer.flush()
			time.sleep(2)


