# sensor_catalogue_registration.py
from app import app
from flask import Flask,request,jsonify
import requests
import sys
import json
import os
from pymongo import MongoClient
from bson.json_util import dumps, loads


# app = Flask(__name__)
appport = 7071
dburl = "mongodb://apurva:user123@cluster0-shard-00-00.p4xv2.mongodb.net:27017,cluster0-shard-00-01.p4xv2.mongodb.net:27017,cluster0-shard-00-02.p4xv2.mongodb.net:27017/IAS_test_1?ssl=true&replicaSet=atlas-auz41v-shard-0&authSource=admin&retryWrites=true&w=majority"
db_name = "IAS_test_1"
collection_name = "sensor_catalogue"

def remove_unnecessary_data(data):

    # for i in data:
    del data["sensor_geolocation"]
    del data["sensor_address"]
    del data["sensor_data_storage_details"]
    del data["sensor_api"]
    return data

def collection_to_json(col):

    cursor = col.find()
    list_cur = list(cursor)
    json_data = dumps(list_cur)
    return json_data


@app.route('/getAllSensorTypes' ,methods=['GET','POST'])
def fun1():

    cluster = MongoClient(dburl)
    db = cluster[db_name]
    collection = db[collection_name]
    result = collection_to_json(collection)
    return result

@app.route('/addSensorType',methods=['GET','POST'])
def fun2(data):

    print('Entered Add Sensor Type function')
    NoneType = type(None)
    incoming_data = data
    # incoming_data = request.get_json(force=True)
    if(type(incoming_data) == NoneType):
        msg={'msg':'No config received'}
        return msg
    user_id = incoming_data["user_id"]
    config = incoming_data["sensor_catalogue_config"]
    config = remove_unnecessary_data(config)
    print("Config - ",config)
    cluster = MongoClient(dburl)
    db = cluster[db_name]
    collection = db[collection_name]

    # for s in config :
    config["user_id"] = user_id
    collection.insert_one(config)

    msg={'msg':'sensor types added'}
    return msg


if __name__ == '__main__':
    app.run(host="0.0.0.0",port=appport)
