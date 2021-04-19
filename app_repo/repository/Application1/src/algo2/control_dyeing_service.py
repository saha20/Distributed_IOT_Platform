from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka import KafkaConsumer
import time
import random
import threading 
import sys
# sewing service, 
# under thread_residual_quantity

def json_serializer(data):
	return json.dumps(data).encode("utf-8")


def main():
    temp_topic = sys.argv[1]
	output_topic = sys.argv[2]

	consumer = KafkaConsumer(str(temp_topic),bootstrap_servers=['127.0.0.1:9092'])
	producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'], value_serializer=json_serializer)

	for message in consumer:
		#input coming is "room temperature"
		s = message.value.decode('utf-8')

		topic_data = s.split('#')
        sensor_id = topic_data[0]
        sensor_type = topic_data[1]
        sensor_data = int(topic_data[2])

		if sensor_type == "temperature" and int(sensor_data)>140 and int(sensor_data)<160:
			data = {
				"action_center":
				{
						"user_display" : "results",
						"sensor_manager": [
							{"sensor_id" : sensor_id},
							{"command": "no operation"}
						],
						"notify_user" :
						{
							['a@mail.com' , '26740404' , '26741997' , 's@gmail.in']
						}
				}
			}
			print(msg)
			producer.send(str(output_topic), data)
			producer.flush() 
			time.sleep(3)
		elif sensor_type == "temperature" and int(sensor_data)<=140:
			data = {
				"action_center":
				{
						"user_display" : "Temperature needs to be increased",
						"sensor_manager": [
							{"sensor_id" : sensor_id},
							{"command": "increase temperature"}
						],
						"notify_user" :
						{
							['a@mail.com' , '26740404' , '26741997' , 's@gmail.in']
						}
				}
			}
			producer.send(str(output_topic), data)
			producer.flush() 
			time.sleep(3)

if __name__ == '__main__':
	#main()
	while True:
		a=1
