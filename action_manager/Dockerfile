FROM python:3-alpine
WORKDIR /myapp
COPY requirements.txt /myapp
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt
ADD actionManager.py .
CMD ["python","-u","actionManager.py"]