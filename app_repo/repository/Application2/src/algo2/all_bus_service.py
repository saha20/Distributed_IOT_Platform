from read_sensor_info import *
# this service will be run for all buses just once instance

def json_serializer(data):
	return json.dumps(data).encode("utf-8")

def bus_near(curr_lat1, curr_long1, curr_lat2, curr_long2, dist_threshold=100):
	p = (float(curr_lat1),float(curr_long1))
	q = (float(curr_lat2),float(curr_long2))
	distance = math.sqrt(sum([(a - b) ** 2 for a, b in zip(p, q)]))
	if(distance<=dist_threshold):
		return True
	return False


def fun():

	college_lat = 0
	college_long = 0 
	bus_location_dict = {}
	bus_gps_sensor_id_dict = {}
	gate_security_email = 'security@iiith'

	temp_topic = sys.argv[1]
	output_topic = sys.argv[2]
	consumer = KafkaConsumer(str(temp_topic),bootstrap_servers=[KAFKA_PLATFORM_IP], auto_offset_reset = "latest")
	producer = KafkaProducer(bootstrap_servers=[KAFKA_PLATFORM_IP], value_serializer=json_serializer)

	for message in consumer:

		s = message.value.decode('utf-8')
		sensor_id, sensor_type, sensor_data = get_sensor_data(s)

		if sensor_type == "gps_iiith_bus":
			print("gps sensor_data : ",sensor_data)
			place_id, curr_bus_lat, curr_bus_long = get_gps_data(sensor_data)
			bus_gps_sensor_id_dict[place_id] = sensor_id

			# usecase 3
			bus_location_dict[place_id] = (curr_bus_lat, curr_bus_long)
			notification_to_send_to_bus_list = []

			for bus in bus_location_dict:
				if(bus == place_id):
					continue
				if(bus_near(bus_location_dict[bus][0],bus_location_dict[bus][1],bus_location_dict[place_id][0],bus_location_dict[place_id][1])):
					notification_to_send_to_bus_list.append(bus)

			number_of_buses_near_this_bus = len(notification_to_send_to_bus_list)
			if(number_of_buses_near_this_bus>=2):
				for bus in notification_to_send_to_bus_list:
					display_msg = "More than 3 buses in this area, "+ bus +" please divert your route."
					msg = message_to_action_manager(display_msg, bus_gps_sensor_id_dict[bus], display_msg, [])
					print(msg)
					producer.send(str(output_topic), msg)
					producer.flush()

			# usecase 4
			if(bus_near(bus_location_dict[bus][0],bus_location_dict[bus][0],college_lat,college_long)):
				display_msg = "Bus number "+place_id+" has reached near the college gate."
				msg = message_to_action_manager(display_msg, "None", "None", [gate_security_email])
				print(msg)
				producer.send(str(output_topic), msg)
				producer.flush()

		time.sleep(3)

if __name__ == '__main__':
	
	t1 = threading.Thread(target = fun, args=())
	t1.start()
