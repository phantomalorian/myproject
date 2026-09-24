FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir pika==1.3.2

COPY notification_worker.py .

CMD ["python", "notification_worker.py"]