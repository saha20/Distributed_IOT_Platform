"""
- Returning lat long as str (see in create_fake_sensor_data())
- format of data being faked+returned is diff for different sensor types. Needs to be read differently in sensor_mgr too.
"""

from sensor_package import *

topics_collection = sensor_topic_details_collection

fake = Faker()

def create_fake_sensor_data(sensor_type, place_id):
	if (sensor_type == 'gps_iiith_bus'):
		return str(place_id) + "," + str(fake.latitude()) + "," + str(fake.longitude())

	elif (sensor_type == 'biometric_iiith_bus'): 
		val = fake.name()
		return str(val)

	elif(sensor_type == 'temperature_iiith_bus'):
		return random.randrange(2, 70)

	elif(sensor_type == 'lux_iiith_bus'):
		return random.randrange(1, 107527)

	elif (sensor_type == 'hook_rotation'):
		return random.randrange(1, 3000)

	elif (sensor_type == 'thread_remaining'): 
		return random.randrange(5, 2000)

	elif(sensor_type == 'stitch_setting'):
		return random.randrange(1, 5000)	

	return 0;

if __name__ == '__main__':

	cluster = MongoClient(dburl)
	db = cluster[db_name]
	collection = db[topics_collection]
	producer = KafkaProducer(bootstrap_servers=[KAFKA_PLATFORM_IP],value_serializer=json_serializer)

	while(True):
		for x in collection.find({}):
			sensor_topic_name = x['sensor_topic_name']
			sensor_type = x['sensor_type']
			place_id = x['place_id']

			data = create_fake_sensor_data(sensor_type, place_id)
			producer.send(sensor_topic_name, data)  # produces data on the topic named sensor_topic_name
			producer.flush()
			time.sleep(1)

	# sensor_ids_sim = get_sensors(number_of_sensors_to_be_simulated)
	# producer = KafkaProducer(bootstrap_servers=[KAFKA_PLATFORM_IP],value_serializer=json_serializer)
	# while True:
	# 	for sensor_no in range(len(sensor_ids_sim)) :
	# 		data = create_fake_sensor_data(sensor_no)
	# 		sensor_topic_name = "sensor_topic_"+sensor_ids_sim[sensor_no] 
	# 		producer.send(sensor_topic_name, data)  # produces a new topic named sensor_topic_name
	# 		producer.flush()
	# 		time.sleep(2)


