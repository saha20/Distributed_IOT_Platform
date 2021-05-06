# sensor_catalogue_registration.py

from flask import Flask,request,jsonify
import requests
import sys
import json
import os
from pymongo import MongoClient
from bson.json_util import dumps, loads

app = Flask(__name__)
appport = 7071
dburl = "mongodb://apurva:user123@cluster0-shard-00-00.p4xv2.mongodb.net:27017,cluster0-shard-00-01.p4xv2.mongodb.net:27017,cluster0-shard-00-02.p4xv2.mongodb.net:27017/IAS_test_1?ssl=true&replicaSet=atlas-auz41v-shard-0&authSource=admin&retryWrites=true&w=majority"
db_name = "IAS_test_1"
collection_name = "sensors_registered"
catalogue_name = "sensor_catalogue"

def remove_unnecessary_data(d):
    # for i in d:
    if(d['sensor_geolocation']['lat'] == "None" or d['sensor_geolocation']['long'] == "None"):
        del d['sensor_geolocation']

    # for i in d:
    for j in d:
        if(j=='sensor_data_storage_details'):
            if((d[j]['kafka']['broker_ip'] =="None" or d[j]['kafka']['topic'] =="None") and (d[j]['mongo_db']['ip'] =="None"  or d[j]['mongo_db']['passwd'] =="None" or d[j]['mongo_db']['document_name'] =="None")):
                print("sensor data info not given")
            else:
                if(d[j]['kafka']['broker_ip'] == "None"):
                    del d[j]['kafka']
                elif(d[j]['mongo_db']['ip'] == "None"):
                    del d[j]['mongo_db']
    return d

def collection_to_json(col):

    cursor = col.find()
    list_cur = list(cursor)
    json_data = dumps(list_cur)
    return json_data


@app.route('/getAllSensors' ,methods=['GET','POST'])
def fun1():

    cluster = MongoClient(dburl)
    db = cluster[db_name]
    collection = db[catalogue_name]
    result = collection_to_json(collection)
    return result

def get_sensor_info(sensor_name):
    cluster = MongoClient(dburl)
    db = cluster[db_name]
    collection = db[catalogue_name]
    sensorInfo = collection.find_one({'sensor_name': sensor_name})
    # print("Document = ",document)
    # sensorInfo = collection[sensor_name]
    return sensorInfo

@app.route('/sensorRegistration',methods=['GET','POST'])
def fun2(data):

    NoneType = type(None)
    incoming_data = data
    # incoming_data = request.get_json(force=True)
    if(type(incoming_data) == NoneType):
        msg={'msg':'No config received'}
        return msg
    user_id = incoming_data["user_id"]
    config = incoming_data["sensor_reg_config"]
    config = remove_unnecessary_data(config)

    cluster = MongoClient(dburl)
    db = cluster[db_name]
    collection = db[collection_name]


    # for s in config :
    config["user_id"] = user_id
    collection.insert_one(config)

    msg={'msg':'sensor registered'}
    return msg


if __name__ == '__main__':
    app.run(host="0.0.0.0",port=appport)
