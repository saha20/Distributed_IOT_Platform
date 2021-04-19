FROM python:3-alpine
WORKDIR /myapp
COPY . /myapp
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt
EXPOSE 9092
CMD ["python","-u","boot_sensor_side.py"]