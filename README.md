#### Distributed_IOT_Platform



app_ui (user gives reuqest)
scheduler( sends request.json to deployer)
deployer (extracts app details and go to app_repo)
app_repo (to get config file)
back to deployer
load_balancer( get worker  node)
back to deployer
sensor__manager( to get temp topic)

Stop all docker 
    sudo docker stop $(sudo docker ps -aq)



Remove all docker
    sudo docker rm $(sudo docker ps -aq)

docker cp Deployer  Service.py deployer:/work_dir/

stopping zookeper
    sudo service stop zookeper
    sudo lsof -i:2181 (finds particular port has any process working on it or not)
    
To get into the terminal of the container
    sudo docker exec -it worker_node_2
    sudo docker exec -it worker_node_2 sh

##### sudo docker build --tag app_ui_docker .
##### sudo docker run --name app_ui_docker_v1 -p 5001:5001  app_ui_docker
##### template update,
##### dyanmic populate applications.
##### zip the applications in test_applications folder and put them in app_repo/repository
    
