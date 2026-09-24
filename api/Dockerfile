FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir Flask==3.0.3 mysql-connector-python==9.0.0 kafka-python==2.0.2 pika==1.3.2

COPY app.py .

CMD ["python", "app.py"]