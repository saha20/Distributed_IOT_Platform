#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 16 04:36:59 2021

@author: aman
"""

from flask import Flask,jsonify
import random
import socket
import os
import time
app = Flask(__name__)
@app.route("/get_load", methods=["GET", "POST"])
def returnServer():
     # get server with lowest request handling
     stream = os.popen('echo $(date +%s%N)')
     tstart = int(stream.read())
     stream = os.popen('echo $(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)')
     cstart = int(stream.read())
     time.sleep(2)
     stream = os.popen('echo $(date +%s%N)')
     tstop = int(stream.read())
     stream = os.popen('echo $(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)')
     cstop = int(stream.read())
     cu=(cstop-cstart)/(tstop-tstart)
     stream = os.popen('echo $(cat /sys/fs/cgroup/memory/memory.usage_in_bytes)')
     mu=int(stream.read())/1024/1024
     mp=mu/200
     return jsonify(
     cpu_load=cu,
     mem_load=mu,
     )
    
if __name__ == "__main__":
    # app.run(host=socket.gethostbyname(socket.gethostname()),port=5000)
    app.run(host= '0.0.0.0',port=5000)
   
