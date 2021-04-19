
# sensor_package.py

from flask import Flask,request,jsonify
import requests
import sys 
import json
import os
import socket
from pymongo import MongoClient
from bson.json_util import dumps, loads
import uuid
from uuid import uuid4
import threading
from kafka import KafkaProducer
from kafka import KafkaConsumer
import time
import random
from faker import Faker

# zookeepeer : 2181
# kafka server (broker) : 9092
# mongodb

dburl = "mongodb://apurva:user123@cluster0-shard-00-00.p4xv2.mongodb.net:27017,cluster0-shard-00-01.p4xv2.mongodb.net:27017,cluster0-shard-00-02.p4xv2.mongodb.net:27017/IAS_test_1?ssl=true&replicaSet=atlas-auz41v-shard-0&authSource=admin&retryWrites=true&w=majority"
db_name = "IAS_test_1"

# kafka

KAFKA_PLATFORM_IP = 'kafka:9092'
action_manager_topic = "actionManager_to_sensorManager"
smgid = "consumer_group_sensor_manager"

# simulator

def get_sensors(number_of_sensors_to_be_simulated):
	sensor_ids_sim = []
	for i in range(number_of_sensors_to_be_simulated+1):
		sensor_ids_sim.append(str(i))
	return sensor_ids_sim


# ports

manager_port = 5050
instance_reg_port = 7072
catalogue_reg_port = 7071

# functions

def json_serializer(data):
	return json.dumps(data).encode("utf-8")


