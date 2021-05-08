
from pymongo import MongoClient
from bson.json_util import dumps, loads
import json


dburl = "mongodb://apurva:user123@cluster0-shard-00-00.p4xv2.mongodb.net:27017,cluster0-shard-00-01.p4xv2.mongodb.net:27017,cluster0-shard-00-02.p4xv2.mongodb.net:27017/IAS_test_1?ssl=true&replicaSet=atlas-auz41v-shard-0&authSource=admin&retryWrites=true&w=majority"
# db_name = "IAS_test_1"
# collection_name = "sensor_catalogue"

def collection_to_json(col):

    cursor = col.find()
    list_cur = list(cursor)
    json_data = dumps(list_cur)
    return json_data



def getDataController(db_name, collection_name):
    sensorIds = []
    commands = []
    cluster = MongoClient(dburl)
    db = cluster[db_name]
    collection = db[collection_name]
    result = collection_to_json(collection)
    result = json.loads(result)
    for row in result:
        sensorIds.append(row['sensor_id'])
        commands.append(row['command'])
    return sensorIds, commands

def getDataUser(db_name, collection_name):
    notifyUsers = []
    userDisplays = []
    cluster = MongoClient(dburl)
    db = cluster[db_name]
    collection = db[collection_name]
    result = collection_to_json(collection)
    result = json.loads(result)
    for row in result:
        notifyUsers.append(row['notify_user'])
        userDisplays.append(row['user_display'])
    return notifyUsers, userDisplays



# getDataUser("IAS_test_1", "user_notifications")
# print('Result = ', result)
