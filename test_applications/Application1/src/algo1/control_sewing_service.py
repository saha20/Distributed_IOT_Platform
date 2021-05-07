from read_sensor_info import *
from service_heartbeat import *

def json_serializer(data):
	return json.dumps(data).encode("utf-8")

def algorithm_thread(temp_topic, output_topic, service_id, json_filename):

	notif_list = parse_notification_info(json_filename)
	hook_rotation_threshold = 25
	thread_quantity_threshold = 5

	consumer = KafkaConsumer(str(temp_topic),bootstrap_servers=[KAFKA_PLATFORM_IP], auto_offset_reset = "latest")
	producer = KafkaProducer(bootstrap_servers=[KAFKA_PLATFORM_IP], value_serializer=json_serializer)
	
	for message in consumer:
		s = message.value.decode('utf-8')
		sensor_id, sensor_type, sensor_data = get_sensor_data(s)

		# Use-case 1, if hook-rotation speed decreases, then send a mail to person to check and update speed.
		if(sensor_type == "hook_rotation"):
			rotation_speed = int(sensor_data)
			if(rotation_speed <= hook_rotation_threshold):
				display_msg = "The rotation speed is " + str(rotation_speed) + " change the speed to above threshold"
				command = "Increase hook rotation speed."
				msg = message_to_action_manager(display_msg, sensor_id, command, notif_list, service_id)
				print(msg)
				producer.send(str(output_topic), msg)
				producer.flush()

		# Use-case 2, if thread-quanitiy reaches below threshold, send a mail/msg to replace with new thread.
		if(sensor_type == "thread_remaining" ):
			thread_quantity = int(sensor_data)
			if(thread_quantity <= thread_quantity_threshold):
				display_msg = "The thread quantity is " + str(thread_quantity) + " insert new thread roll"
				command = "Replace thread."
				msg = message_to_action_manager(display_msg, sensor_id, command, notif_list, service_id)
				print(msg)
				producer.send(str(output_topic), msg)
				producer.flush()

	time.sleep(3)


if __name__ == '__main__':

	temp_topic = sys.argv[1]
	output_topic = sys.argv[2]
	service_id = sys.argv[3]
	json_filename = sys.argv[4]

	heartbeat_thread = threading.Thread(target = sendHeartbeat, args = (service_id,))
	heartbeat_thread.start()

	service_thread = threading.Thread(target = algorithm_thread, args = (temp_topic, output_topic, service_id, json_filename, ))
	service_thread.start()
	

