from pymongo import MongoClient
# mongodb+srv://ram1729:<1729>@cluster0.nfo1g.mongodb.net/myFirstDatabase?retryWrites=true&w=majority

cluster = MongoClient("mongodb://apurva:user123@cluster0-shard-00-00.p4xv2.mongodb.net:27017,cluster0-shard-00-01.p4xv2.mongodb.net:27017,cluster0-shard-00-02.p4xv2.mongodb.net:27017/IAS_test_1?ssl=true&replicaSet=atlas-auz41v-shard-0&authSource=admin&retryWrites=true&w=majority")
db = cluster["IAS_test_1"]
# collection = db["test"]
print("database:", db.list_collection_names())