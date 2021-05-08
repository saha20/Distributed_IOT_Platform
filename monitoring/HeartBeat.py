from kafka import KafkaConsumer
from json import loads
import json
import threading
import time , requests
import copy
from time import sleep
import collections
from collections import defaultdict

heart_beat_dict = dict()
heart_beat_dict_old = dict()
heart_beat_dict_new = dict()
heart_beat_status_dict = dict()


def callfaultTolerance(module_name):
    print(f"{module_name} FAILED !!!")
    to_send={}
    to_send['module_name'] = module_name

    json_object = json.dumps(to_send)
    fault_tolerance_url = 'http://host.docker.internal:6969/faulty'
    try :
        resp = requests.post(fault_tolerance_url, json = to_send)
        print(resp.text)
    except :
        print("Can't connect to Fault Tolerance Module")
    # communicate fault tolerance 

def update_status() :
    global heart_beat_dict_old, heart_beat_dict_new, heart_beat_status_dict
    while True :
        heart_beat_dict_old = copy.deepcopy(heart_beat_dict)
        sleep(3)        
        heart_beat_dict_new = copy.deepcopy(heart_beat_dict)
        for module_name, time_stamp in heart_beat_dict_new.items() :
            if module_name in heart_beat_dict_old.keys() :
                if collections.Counter(heart_beat_dict_old[module_name]) == collections.Counter(heart_beat_dict_new[module_name]):
                    heart_beat_status_dict[module_name] = 'D'
                    callfaultTolerance(module_name)
                else :
                    heart_beat_status_dict[module_name] = 'A'
            else :          
                heart_beat_status_dict[module_name] = 'A'
        # print("===============heart_beat_dict=============")
        # print(heart_beat_dict)    
        print("\n\n")
        print("===============heart_beat_status_dict=============")
        print(heart_beat_status_dict)   
        print("\n=========================================\n")

def get_heartbeat() :
    global heart_beat_dict_old, heart_beat_dict_new, heart_beat_status_dict
    consumer = KafkaConsumer(
        'HeartBeat',
         bootstrap_servers=['host.docker.internal:9092'],
         auto_offset_reset='latest',
         enable_auto_commit=True,
         group_id='my-group',
         value_deserializer=lambda x: loads(x.decode('utf-8')))

    for message in consumer:
        module_name = message.value['module']
        time_stamp = message.value['ts']
        if module_name in heart_beat_dict.keys() : 
            if len(heart_beat_dict[module_name]) >= 5 :
                heart_beat_dict[module_name].pop(0)
            heart_beat_dict[module_name].append(time_stamp)
        else :
            heart_beat_dict[module_name] = list()
            heart_beat_dict[module_name].append(time_stamp)
 


if __name__ == '__main__':
    thread1 = threading.Thread(target = update_status)
    thread1.start()
    get_heartbeat()
            