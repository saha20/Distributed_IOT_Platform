
from sensor_package import *

def insert_place_details():
    cluster = MongoClient(dburl)
    db = cluster[db_name]
    collection = db[place_details_collection]
    config_new = {}
    config_new[""]

    collection.insert_one()
