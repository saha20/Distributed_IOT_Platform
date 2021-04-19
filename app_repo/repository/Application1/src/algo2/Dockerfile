FROM python:3-alpine
RUN pip3 install kafka-python 

COPY ./ ./

ENTRYPOINT python3 -u control_dyeing_service.py service1 action_service_topic 