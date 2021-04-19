# sensor_catalogue_registration.py

from sensor_package import * 

app = Flask(__name__)
appport = instance_reg_port
collection_name = "sensors_registered"

def get_all_sensor_types():

    cat_coll = "sensor_catalogue"
    cluster = MongoClient(dburl)
    db = cluster[db_name]
    collection = db[cat_coll]
    result = collection_to_json(collection)
    result = json.loads(result)
    type_list = []

    for row in result:
        print(row)
        type_list.append(row["sensor_name"])

    return type_list

def validate_if_type_present(config, sensor_type_list):

    s_type = config["sensor_name"]
    if(s_type not in sensor_type_list) :
        return True, s_type
    else :
        return False, s_type

def remove_unnecessary_data(d):
    for i in d:
        if(d[i]['sensor_geolocation']['lat'] == "None" or d[i]['sensor_geolocation']['long'] == "None"):
            del d[i]['sensor_geolocation']

    for i in d:
        for j in d[i]:
            if(j=='sensor_data_storage_details'):
                if((d[i][j]['kafka']['broker_ip'] =="None" or d[i][j]['kafka']['topic'] =="None") and (d[i][j]['mongo_db']['ip'] =="None"  or d[i][j]['mongo_db']['passwd'] =="None" or d[i][j]['mongo_db']['document_name'] =="None")):
                    print("sensor data info not given")
                else:
                    if(d[i][j]['kafka']['broker_ip'] == "None"):
                        del d[i][j]['kafka']
                    elif(d[i][j]['mongo_db']['ip'] == "None"):
                        del d[i][j]['mongo_db']
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
    collection = db[collection_name]
    result = collection_to_json(collection)
    return result

@app.route('/sensorRegistration',methods=['GET','POST'])
def fun2():

    NoneType = type(None)
    incoming_data = request.get_json(force=True)
    if(type(incoming_data) == NoneType):
        msg={'msg':'No config received'}
        return msg
    user_id = incoming_data["user_id"]
    config = incoming_data["sensor_reg_config"]
    config = remove_unnecessary_data(config)

    present_sensor_types = get_all_sensor_types()
    print(present_sensor_types)

    cluster = MongoClient(dburl)
    db = cluster[db_name]
    collection = db[collection_name]

    sensor_type_not_registered = ""
    error_flag = False

    for s in config :
        error, sensor_type = validate_if_type_present(config[s],present_sensor_types)
        if(error == False):
            unique_id = str(uuid4())
            print("unique_id :", unique_id)
            config[s]["sensor_id"] = unique_id    # change for sensor_id inclusion
            config[s]["user_id"] = user_id
            collection.insert_one(config[s])
        else:
            sensor_type_not_registered = sensor_type +", "+sensor_type_not_registered
            error_flag = True

    msg = "sensors registered"
    if error_flag:
        msg = sensor_type_not_registered + " are the sensors types not registered. Please register before use."

    resmsg = {'msg': msg}
    return resmsg
  

if __name__ == '__main__':
    print("Inside sensor_catalogue_registration")
    # app.run(host=socket.gethostbyname(socket.gethostname()),port=appport)
    app.run(host="0.0.0.0",port=appport)
