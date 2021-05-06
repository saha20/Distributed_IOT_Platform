from read_sensor_info import *
# this service will be run seperately for every bus

def json_serializer(data):
	return json.dumps(data).encode("utf-8")

def calculate_fare(curr_bus_lat, curr_bus_long, college_lat, college_long):
	p = (float(curr_bus_lat),float(curr_bus_long))
	q = (float(college_lat),float(college_long))
	# distance = math.dist(p,q)
	distance = math.sqrt(sum([(a - b) ** 2 for a, b in zip(p, q)]))
	return math.ceil(distance)

def fun(temp_topic, output_topic, serviceid, json_filename):

	new_passangers = []
	passangers_in_bus_count = 0
	curr_bus_lat = 25
	curr_bus_long = 25
	college_lat = 0
	college_long = 0 
	lux_limit = 50
	temp_limit = 35
	light_on = False
	ac_on = False
	notif_list = parse_notification_info(json_filename)

	consumer = KafkaConsumer(str(temp_topic),bootstrap_servers=[KAFKA_PLATFORM_IP], auto_offset_reset = "latest")
	producer = KafkaProducer(bootstrap_servers=[KAFKA_PLATFORM_IP], value_serializer=json_serializer)

	for message in consumer:
		s = message.value.decode('utf-8')
		sensor_id, sensor_type, sensor_data = get_sensor_data(s)
		# print("sensor_id ", sensor_id)
		# print("sensor_type ", sensor_type)
		# print("sensor_data ", sensor_data)

		if sensor_type == "biometric_iiith_bus":
			new_passangers.append(sensor_data)
			passangers_in_bus_count = passangers_in_bus_count + 1

		if sensor_type == "gps_iiith_bus":
			print("gps sensor_data : ",sensor_data)
			place_id, curr_bus_lat, curr_bus_long = get_gps_data(sensor_data)
			fare_amount = calculate_fare(curr_bus_lat, curr_bus_long, college_lat, college_long)
			for person in new_passangers:
				display_msg = "Collect "+str(fare_amount)+" rs from "+person+" in "+place_id+"."
				msg = message_to_action_manager(display_msg, "None", "None", notif_list)
				print(msg)
				producer.send(str(output_topic), msg)
				producer.flush()
			new_passangers = []

		if sensor_type == "temperature_iiith_bus":
			temp_value = int(sensor_data)
			if(temp_value >= temp_limit and passangers_in_bus_count>=1 and ac_on==False):
				display_msg = "The temperature is "+str(temp_value)+". Switching on the A/C."
				command = "turn on a/c"
				ac_on = True
				msg = message_to_action_manager(display_msg, sensor_id, command, [])
				print(msg)
				producer.send(str(output_topic), msg)
				producer.flush() 
				
			if(temp_value <= 28 and ac_on==True):
				display_msg = "The temperature is "+str(temp_value)+". Switching off the A/C."
				command = "turn off a/c"
				ac_on = False
				msg = message_to_action_manager(display_msg, sensor_id, command, [])
				print(msg)
				producer.send(str(output_topic), msg)
				producer.flush() 
			
		if sensor_type == "lux_iiith_bus":
			lux_value = int(sensor_data)
			if(lux_value <= lux_limit and passangers_in_bus_count>=1 and light_on==False):
				display_msg = "The lux rating is "+str(lux_value)+". Switching on the bus lights."
				command = "turn on lights"
				notif_list = []
				light_on = True
				msg = message_to_action_manager(display_msg, sensor_id, command, [])
				print(msg)
				producer.send(str(output_topic), msg)
				producer.flush() 

			if(lux_value >= 100 and light_on==True):
				display_msg = "The lux rating is "+str(lux_value)+". Switching off the bus lights."
				command = "turn off lights"
				notif_list = []
				light_on = False
				msg = message_to_action_manager(display_msg, sensor_id, command, [])
				print(msg)
				producer.send(str(output_topic), msg)
				producer.flush() 

		time.sleep(3)

if __name__ == '__main__':

	temp_topic = sys.argv[1]
	output_topic = sys.argv[2]
	serviceid = sys.argv[3]
	json_filename = sys.argv[4]
	t1 = threading.Thread(target = fun, args=(temp_topic, output_topic, serviceid, json_filename,))
	t1.start()