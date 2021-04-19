# application_consumer.py

from sensor_package import * 

# zookeepeer : 2181
# kafka server (broker) : 9092
# first run zookeeper then sever
# create fake data (sensor uploading)

# conf needed by producer :

# bootstrap_servers, value_serializer

# conf needed by a consumer :

# topic, bootstrap_servers, group_id (if not given is set to some default)
# Consumer is affiliated to a consumer group
# latest :  to start consuming the sensor data from  the latest available

# starting and stopping consumer + producer


def json_deserializer(msg):
	return json.loads(msg.value)

if __name__ == '__main__':
	
	producer_topic = "sensor_topic_15"
	consumer = KafkaConsumer(producer_topic, bootstrap_servers=[KAFKA_PLATFORM_IP], auto_offset_reset = "latest", group_id=smgid)
	for msg in consumer:
		print(json.loads(msg.value))




