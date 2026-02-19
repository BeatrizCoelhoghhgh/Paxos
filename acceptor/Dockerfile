
FROM python:3.11-slim
WORKDIR /app
COPY acceptor.py /app/acceptor.py
RUN pip install flask requests prometheus_client
RUN pip install --no-cache-dir flask requests
EXPOSE 8000
CMD ["python", "-u", "acceptor.py"]
