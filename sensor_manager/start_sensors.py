# start_sensors.py

from sensor_package import *
fake = Faker()
number_of_sensors_to_be_simulated = 15

def create_fake_sensor_data(sensor_no):
	dt = 'int'
	if (dt == 'int'):
		return random.randrange(0,2000)
	elif (dt == 'float'): 
		return random.uniform(2.5, 50.0)
	elif(dt == 'string'):
		return fake.name()
	return 0;

if __name__ == '__main__':

	sensor_ids_sim = get_sensors(number_of_sensors_to_be_simulated)
	producer = KafkaProducer(bootstrap_servers=[KAFKA_PLATFORM_IP],value_serializer=json_serializer)
	while True:
		for sensor_no in range(len(sensor_ids_sim)) :
			data = create_fake_sensor_data(sensor_no)
			sensor_topic_name = "sensor_topic_"+sensor_ids_sim[sensor_no] 
			producer.send(sensor_topic_name, data)  # produces a new topic named sensor_topic_name
			producer.flush()
			time.sleep(2)


