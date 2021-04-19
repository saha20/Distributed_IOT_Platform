# action_manager.py


from sensor_package import *

if __name__ == '__main__':

	value = "{ 'sensor_id1' : 'Reduce AC temperature' }"
	producer = KafkaProducer(bootstrap_servers=[KAFKA_PLATFORM_IP])
	while 1==1:
		producer.send(action_manager_topic, bytes(value,"utf-8"))
		producer.flush()
		time.sleep(10)

