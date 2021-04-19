FROM python:3-alpine
WORKDIR /work_dir
COPY . /work_dir
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt
EXPOSE 55555
CMD ["python","-u","Load_balancer.py"]

