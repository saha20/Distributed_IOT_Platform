FROM python:3.6-stretch
WORKDIR /work_dir
COPY . /work_dir
RUN apt-get update \
    && apt-get -y install postgresql postgresql-client gcc python3-dev musl-dev libffi-dev openssl libssl-dev
RUN pip3 install -r requirements.txt
EXPOSE 5001
ENTRYPOINT [ "python" ]
CMD [ "app.py" ]
