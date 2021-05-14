#### Distributed IOT Platform

[Contributors][contributors-shield][contributors-url]

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

### About The Project

Use this space to demonstrate about the project.

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


<!-- USAGE EXAMPLES -->
### Usage

Use this space to show useful examples of how a project can be used. Additional screenshots, code examples and demos work well in this space. You may also link to more resources.

### Flow

Use this space to show useful examples of how a project can be used. Additional screenshots, code examples and demos work well in this space. You may also link to more resources.

_For more examples, please refer to the [Documentation](https://example.com)_

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
    
<!-- MARKDOWN LINKS & IMAGES -->
[contributors-url]: https://github.com/saha20/Distributed_IOT_Platform/graphs/contributors
