from read_sensor_info import *
import sendHeartbeat as sh
# this service will be run for all buses just once instance

def json_serializer(data):
	return json.dumps(data).encode("utf-8")

def get_barricade_lat_long():
	l1 = [0,30,60,90]
	l2 = [0,30,60,90]
	return l1,l2

def bus_near(curr_lat1, curr_long1, curr_lat2, curr_long2, dist_threshold=500):
	p = (float(curr_lat1),float(curr_long1))
	q = (float(curr_lat2),float(curr_long2))
	distance = math.sqrt(sum([(a - b) ** 2 for a, b in zip(p, q)]))
	if(distance<=dist_threshold):
		return True
	return False

def fun(temp_topic, output_topic, serviceid, json_filename):

	barricade_lat_list, barricade_long_list  = get_barricade_lat_long() 
	notif_list = parse_notification_info(json_filename)

	consumer = KafkaConsumer(str(temp_topic),bootstrap_servers=[KAFKA_PLATFORM_IP], auto_offset_reset = "latest")
	producer = KafkaProducer(bootstrap_servers=[KAFKA_PLATFORM_IP], value_serializer=json_serializer)

	for message in consumer:

		s = message.value.decode('utf-8')
		sensor_id, sensor_type, sensor_data = get_sensor_data(s)

		if sensor_type == "gps_iiith_bus":
			print("gps sensor_data : ",sensor_data)
			place_id, curr_bus_lat, curr_bus_long = get_gps_data(sensor_data)

			# usecase 4
			for i in range(len(barricade_lat_list)):
				b_lat = barricade_lat_list[i]
				b_long = barricade_long_list[i]
				if(bus_near(curr_bus_lat,curr_bus_long,b_lat,b_long)):
					display_msg = "Bus number "+place_id+" has reached near the college barricade "+str(i)+"."
					msg = message_to_action_manager(display_msg, "None", "None", notif_list)
					print(msg)
					producer.send(str(output_topic), msg)
					producer.flush()

		time.sleep(3)

if __name__ == '__main__':

	temp_topic = sys.argv[1]
	output_topic = sys.argv[2]
	service_id = sys.argv[3]
	json_filename = sys.argv[4]

	application_thread = threading.Thread(target = fun, args=(temp_topic, output_topic, service_id, json_filename,))
	application_thread.start()
	heartbeat_thread = threading.Thread(target = sh.sendHeartbeat, args = (service_id,))
	heartbeat_thread.start()
	

