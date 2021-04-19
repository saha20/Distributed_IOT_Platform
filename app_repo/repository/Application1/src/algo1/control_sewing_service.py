# from kafka import KafkaProducer
# from kafka.errors import KafkaError
# from kafka import KafkaConsumer
import time
import random
# import threading 
import sys
import json
import time , threading , json , os, sys
from time import sleep

# from kafka import KafkaProducer
# sewing service, 
# under thread_residual_quantity

# def json_serializer(data):
# 	return json.dumps(data).encode("utf-8")

# def main():
# 	temp_topic = sys.argv[1]
# 	output_topic = sys.argv[2]

# 	consumer = KafkaConsumer(str(temp_topic),bootstrap_servers=['127.0.0.1:9092'])
# 	test_producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
# 	test_producer.send('output_topic', temp_topic)
# 	producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer =json_serializer)
# 		#input coming is "room temperature"	
# 	for message in consumer:																																																																		
# 		s = message.value.decode('utf-8')
		
# 		test_producer.send('output_topic', 'I got messagge from sensor manager')

# 	# for i in range(1,10):
# 	# 	# data = output_topic+'#'+str(random.randint(40,200))
# 	# 	data = 	{
# 	# 		   "action_center":{
# 	# 			  "user_display":"results",
# 	# 			  "sensor_manager":[
# 	# 				 {
# 	# 					"sensor_id":'123'
# 	# 				 },
# 	# 				 {
# 	# 					"command":"insert new threadss"
# 	# 				 }
# 	# 			  ],
# 	# 			  "notify_users":[
# 	# 				 "souptikmondal2014@gmail.com",
# 	# 				 "7001275910",
# 	# 				 "7980908816"
# 	# 			  ]
# 	# 		   }
# 	# 		}
# 	# 	print('\n\b sensor manager')
# 	# 	# time.sleep(5)
# 	# 	print("\n\b send data")
# 	# 	producer.send(output_topic, data)
# 	# time.sleep(1000)
	
# 		topic_data = s.split('#')
# 		sensor_id = topic_data[0]
# 		sensor_type = topic_data[1]
# 		sensor_data = int(topic_data[2])

# 		if sensor_type == "under thread residual quantity" and int(sensor_data)<2:
# 			data = {
# 				"action_center":
# 				{
# 						"user_display" : "results",
# 						"sensor_manager": [
# 							{"sensor_id" : sensor_id},
# 							{"command": "insert new thread"}
# 						],
# 						"notify_user" :
# 						{
# 							['a@mail.com' , '26740404' , '26741997' , 's@gmail.in']
# 						}
# 				}
# 			}
# 			print(msg)
# 			producer.send(str(output_topic), data)
# 			producer.flush() 
# 			time.sleep(3)
# 		elif sensor_type == "under thread residual quantity" and int(sensor_data)>=2:
# 			data = {
# 				"action_center":
# 				{
# 						"user_display" : "Under residual thread is okay",
# 						"sensor_manager": [
# 						],
# 						"notify_user" :
# 						{
# 							['a@mail.com' , '26740404' , '26741997' , 's@gmail.in']
# 						}
# 				}
# 			}
# 			producer.send(str(output_topic), data)
# 			producer.flush() 
# 			time.sleep(3)
# 		else:	
# 			data = {
# 				"action_center":
# 				{
# 						"user_display" : "Thread couldn't detected, insert new thread",
# 						"sensor_manager": [
# 							{"sensor_id" : "insert thread to start machine"},
# 						],
# 						"notify_user" :
# 						{
# 							['a@mail.com' , '26740404' , '26741997' , 's@gmail.in']
# 						}
# 				}
# 			}
# 			producer.send(str(output_topic), data)
# 			producer.flush() 
# 			time.sleep(3)

if __name__ == '__main__':
	# main()
	os.system('mkdir "101"')
	while True:
		a=1
	

