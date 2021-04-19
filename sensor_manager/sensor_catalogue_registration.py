# sensor_catalogue_registration.py

from sensor_package import * 

app = Flask(__name__)
appport = catalogue_reg_port
collection_name = "sensor_catalogue"

def remove_unnecessary_data(data):
    
    for i in data:
        del data[i]["sensor_geolocation"]
        del data[i]["sensor_address"]
        del data[i]["sensor_data_storage_details"]
        del data[i]["sensor_api"]
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
def fun2():

    NoneType = type(None)
    incoming_data = request.get_json(force=True)
    if(type(incoming_data) == NoneType):
        msg={'msg':'No config received'}
        return msg
    user_id = incoming_data["user_id"]
    config = incoming_data["sensor_catalogue_config"]
    config = remove_unnecessary_data(config)

    cluster = MongoClient(dburl)
    db = cluster[db_name]
    collection = db[collection_name]
    
    for s in config :
        config[s]["user_id"] = user_id
        collection.insert_one(config[s])

    msg={'msg':'sensor types added'}
    return msg
  

if __name__ == '__main__':
    app.run(host="0.0.0.0",port=appport)
    print("Imside sensor_catalogue_registration")
    # app.run(host=socket.gethostbyname(socket.gethostname()),port=appport)
