# Distributed_IOT_Platform



allocatedPool":{   
    "kafka" : "127.0.0.1:9092" ,
   “app_ui” : “127.0.0.1:9999”
    “app_repo” : 7007,
    "heart_beat_manager" : "127.0.0.1:50000" , (no use though)
    "scheduler" : "127.0.0.1:13337",
    "deployer" : "127.0.0.1:5001",
  “service_lm_port”	:	“127.0.0.1:8089”
    "load_balancer" : "127.0.0.1:55555",
    "action_manager" : "127.0.0.1:51000" ,(no used though)
    "sensor_manager" : "127.0.0.1:5050",
    "instance_reg_port" : "127.0.0.1:7072" ,
    "catalogue_reg_port" : "127.0.0.1:7071"
“worker_nodes” : “some_ip:5000”
	“fault_tolerance : 6969

    }

# sudo docker build --tag app_ui_docker .
# sudo docker run --name app_ui_docker_v1 -p 5001:9999 app_ui_docker
