from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka import KafkaConsumer
import time
import random
import threading 
import sys
import math
import json
KAFKA_PLATFORM_IP = 'localhost:9092'
# KAFKA_PLATFORM_IP = 'kafka:9092'
# RPC file to read sensor info
# read_sensor_info



def get_sensor_data(s):
	# print(s)
	s = str(s)
	s = s.replace('\"','')
	s = s.replace('\\','')
	topic_data = s.split('#')

	sensor_id = topic_data[0]
	sensor_type = topic_data[1]
	sensor_data = topic_data[2]
	return sensor_id, sensor_type, sensor_data


def get_gps_data(s):
	data = s.split(',')
	return data[0],data[1],data[2]  
	# eg : bus_10,29,45 --> place_id,lat,long

def parse_notification_info(filename):

	notif_list = []
	f = open("./"+filename)
	json_object = json.load(f)
	for l in json_object['notify_user']:
		notif_list.append(str(l))

	return notif_list


def message_to_action_manager(display_msg, sensor_id, command, notif_list):

	notif_string = json.dumps(notif_list)
	print("notif_string ",notif_string)

	output_request_to_actionmanager = {
		"action_center":
					{
							"user_display" : display_msg,
							"sensor_manager": [
								{"sensor_id" : sensor_id},
								{"command": command}
							],
							"notify_user" : notif_string
					}
	}

	return output_request_to_actionmanager
