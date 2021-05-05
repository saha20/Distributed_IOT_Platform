FROM python:3-alpine
WORKDIR /myapp
COPY . /myapp
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt
CMD ["python","-u","ServiceLifeCycleManager.py"]
