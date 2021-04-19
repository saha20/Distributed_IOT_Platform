FROM rastasheep/ubuntu-sshd
WORKDIR /work_dir
COPY . /work_dir

RUN apt-get update && \
    apt install -y iproute2 && \
    apt-get install -y python3-pip && \
    pip3 install -U pip setuptools
RUN pip3 install --no-cache-dir -r requirements.txt


CMD ["python3","DeployerService.py"]
