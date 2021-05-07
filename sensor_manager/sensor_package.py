
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
KAFKA_PLATFORM_IP = 'host.docker.internal:9092'
# KAFKA_PLATFORM_IP = 'localhost:9092'
action_manager_topic = "actionManager_to_sensorManager"

# ports

manager_port = 5050
instance_reg_port = 7072
catalogue_reg_port = 7071

# sensor_catalogue_collection = "sensor_catalogue_demo"
# sensor_instance_collection = "sensors_registered_demo"
sensor_catalogue_collection = "sensor_catalogue"
sensor_instance_collection = "sensors_registered"
place_details_collection = "place_collection"
sensor_topic_details_collection = "sensor_topics_collection"

# functions

def json_serializer(data):
	return json.dumps(data).encode("utf-8")

def collection_to_json(col):
	cursor = col.find()
	list_cur = list(cursor)
	json_data = dumps(list_cur)
	return json_data

