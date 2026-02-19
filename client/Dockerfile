FROM python:3.11-slim
WORKDIR /app
COPY client.py /app/
RUN pip install flask requests prometheus_client
RUN pip install --no-cache-dir flask requests
EXPOSE 5000
CMD ["python", "-u", "client.py"]
