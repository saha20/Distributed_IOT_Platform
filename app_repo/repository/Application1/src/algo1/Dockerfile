FROM python:3-alpine
RUN pip3 install kafka-python 

COPY ./ ./

ENTRYPOINT python3 -u ./control_sewing_service.py 1539 action_service_topic 