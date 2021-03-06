#### Distributed IOT Platform

[![Contributors][contributors-shield]][contributors-url]

<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#about-the-project">About The Project</a></li>
    <li><a href="#prerequisites">Prerequisites</a></li>
    <li><a href="#installation">Installation</a></li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#flow">Flow</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgements">Acknowledgements</a></li>
  </ol>
</details>

### About the Project

This is a distributed IoT Platform where algorithm can be developed, this algorithm can use any sensor which are registered on platform. This sensor can also be registered on the platform by admin/developer.


<!-- [![Container Running Screenshot][product-screenshot]]
<img src="https://raw.githubusercontent.com/saha20/Distributed_IOT_Platform/main/project_images/containers.png" alt="banner"> -->

##### Screenshot of Dashboard
![DashBoard of IoT Platform][dashboard]

##### Screenshot of Microservices Running
![Container Running Screenshot][product-screenshot]

### Platform Architecture
![Architecture][architecture]

More Information About Platform Architecture ```project_documents/group_design_document.pdf```

### Built With

* [Bootstrap](https://getbootstrap.com)
* [Python3](https://www.python.org/)
* [Flask](https://flask.palletsprojects.com/en/2.0.x/)
* [Kafka](https://kafka.apache.org/)
* [Docker](https://www.docker.com/)

### Installation

1. Set Up Free MongoDB Service at [MongoDB](https://www.mongodb.com/cloud/atlas)
2. Clone the repo
   ```sh
   git clone https://github.com/saha20/Distributed_IOT_Platform
   ```
3. Install docker
   Guide available at [Docker Installation](https://docs.docker.com/engine/install/ubuntu/)

### Running

1. For Windows ``` python bootstrap_windows.py```
2. For Linux ```python3 bootstrap_linux.py```


### Running Individual Containers

1. cd into service folder
2. ```sudo docker build --tag app_ui_docker .```
3. ```sudo docker run --name app_ui_docker_v1 -p port_no:port_no  app_ui_docker```

### Running by Downloading Images from Docker Hub

use this space for guide to download images and running directly from docker hub 


### Ports used by different modules

1. kafka : 9092  
2. zookeeper : 2181 
3. app-ui : 9999
4. app_repo : 7007
5. deployer : 5001
6. service_lm : 8089
7. scheduler :13337
8. load_balancer : 55555
9. sensor_manager : 5050
10. instance_reg_port : 7072
11. catalogue_reg_port : 7071
12. fault_tolerance : 6969
13. worker_nodes : 5000
<!-- USAGE EXAMPLES -->
### Usage

Use this space to show useful examples of how a project can be used. Additional screenshots, code examples and demos work well in this space. You may also link to more resources.

### Flow

Use this space to show the flow of application

app_ui (user gives reuqest)
scheduler( sends request.json to deployer)
deployer (extracts app details and go to app_repo)
app_repo (to get config file)
back to deployer
load_balancer( get worker  node)
back to deployer
sensor__manager( to get temp topic)

### Some Useful Commands

1. stopping all images running ```sudo docker stop $(sudo docker ps -aq)``` (remove sudo for windows)
2. removing all images running ```sudo docker rm $(sudo docker ps -aq)``` (remove sudo for windows)
3. copying files/folder from localost to docker container ```docker cp Deployer  Service.py deployer:/path_insisde_container/```
4. stopping zookeper ```sudo service stop zookeper```
5. Finding service running on port 2181 ```sudo lsof -i:2181 ```
6. Exec into container ```sudo docker exec -it worker_node_2 sh```
7. Removing all container, images, network that are dangling ```docker system prune```
8. 

    
<!-- MARKDOWN LINKS & IMAGES -->
[contributors-shield]: https://img.shields.io/github/contributors/othneildrew/Best-README-Template.svg?style=for-the-badge
[contributors-url]: https://github.com/saha20/Distributed_IOT_Platform/graphs/contributors
[product-screenshot]: project_images/containers.png
[dashboard]: project_images/dashboard.png
[architecture]: project_images/architecture.png
