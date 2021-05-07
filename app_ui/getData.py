
from pymongo import MongoClient
from bson.json_util import dumps, loads


dburl = "mongodb://apurva:user123@cluster0-shard-00-00.p4xv2.mongodb.net:27017,cluster0-shard-00-01.p4xv2.mongodb.net:27017,cluster0-shard-00-02.p4xv2.mongodb.net:27017/IAS_test_1?ssl=true&replicaSet=atlas-auz41v-shard-0&authSource=admin&retryWrites=true&w=majority"
# db_name = "IAS_test_1"
# collection_name = "sensor_catalogue"

def collection_to_json(col):

    cursor = col.find()
    list_cur = list(cursor)
    json_data = dumps(list_cur)
    return json_data

def getData(db_name, collection_name):
    cluster = MongoClient(dburl)
    db = cluster[db_name]
    collection = db[collection_name]
    result = collection_to_json(collection)
    return result



# result = getData("IAS_test_1", "sensor_catalogue")
# print('Result = ', result)
