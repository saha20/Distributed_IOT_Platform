FROM       ubuntu:18.04
MAINTAINER Aleksandar Diklic "https://github.com/rastasheep"

RUN apt-get update

RUN apt-get install -y openssh-server
RUN mkdir /var/run/sshd

RUN echo 'root:root' |chpasswd

RUN sed -ri 's/^#?PermitRootLogin\s+.*/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed -ri 's/UsePAM yes/#UsePAM yes/g' /etc/ssh/sshd_config

RUN mkdir /root/.ssh

RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN apt-get update && \
    apt install -y iproute2 && \
    apt-get install -y python3-pip && \
    pip3 install -U pip setuptools
    
COPY . /load_server

RUN pip3 install --no-cache-dir -r load_server/requirements.txt

EXPOSE 22
EXPOSE 5000

CMD	   python3 -u load_server/container_server.py &
CMD    /usr/sbin/sshd -D
#CMD    bash /load_server/script.sh && /usr/sbin/sshd -D
#["bash","script.sh ","&&","/usr/sbin/sshd", "-D"]







