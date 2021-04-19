FROM python:3-alpine
WORKDIR /myapp
COPY . /myapp
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt
EXPOSE 13337
ENV TZ=Asia/Kolkata
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
CMD ["python3","-u","scheduler_main.py", "deployer"]
