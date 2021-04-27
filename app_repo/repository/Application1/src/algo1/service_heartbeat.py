from flask import Flask, jsonify, request
import requests as rq
import time

slm_url = 'http://service_life_manager:8089/listen_services_heartbeat'

def sendHeartbeat(service_id):
	while True:
		
		# print("heartbeat sent -- ", service_id)
		r = {'service_id':str(service_id)}
		rsp = rq.post(slm_url, json = r)
		print("hitting again")
		time.sleep(2)
