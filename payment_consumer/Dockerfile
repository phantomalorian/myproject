FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir kafka-python==2.0.2

COPY payment_consumer.py .

CMD ["python", "payment_consumer.py"]