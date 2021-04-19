#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 16 04:36:59 2021

@author: aman
"""

from flask import Flask,jsonify
import random
import socket
app = Flask(__name__)
@app.route("/get_load", methods=["GET", "POST"])
def returnServer():
     # get server with lowest request handling
     return jsonify(
     cpu_load=random.randint(0, 100),
     mem_load=random.randint(0, 100),
     )
    
if __name__ == "__main__":
    app.run(host=socket.gethostbyname(socket.gethostname()),port=5000)
   
