import os, json, logging
from flask import Flask, request, jsonify
import mysql.connector
from kafka import KafkaProducer
import pika

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("api")
app = Flask(__name__)

DB = dict(
    host=os.getenv("MYSQL_HOST", "mysql"),
    user=os.getenv("MYSQL_USER", "shop"),
    password=os.getenv("MYSQL_PASSWORD", "shop"),
    database=os.getenv("MYSQL_DATABASE", "shop"),
)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
RABBIT = dict(
    host=os.getenv("RABBIT_HOST", "rabbitmq"),
    user=os.getenv("RABBIT_USER", "app"),
    password=os.getenv("RABBIT_PASS", "app"),
)
QUEUE = "order-notifications"
TOPIC = "order-events"

_producer = None
def get_producer():
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            key_serializer=lambda k: str(k).encode(),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(),
            acks="all",
        )
    return _producer

def notify(text):
    params = pika.ConnectionParameters(
        host=RABBIT["host"],
        credentials=pika.PlainCredentials(RABBIT["user"], RABBIT["password"]),
    )
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.basic_publish(
        exchange="", routing_key=QUEUE, body=text.encode(),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    conn.close()

@app.get("/api/health")
def health():
    return jsonify(status="ok")

@app.post("/api/orders")
def create_order():
    data = request.get_json(silent=True) or {}
    customer = str(data.get("customer", "")).strip()
    product = str(data.get("product", "")).strip()
    try:
        amount = round(float(data.get("amount")), 2)
    except (TypeError, ValueError):
        return jsonify(error="Некорректная сумма"), 400
    if not customer or not product or amount <= 0:
        return jsonify(error="Заполните все поля корректно"), 400

    try:
        # 1. MySQL
        db = mysql.connector.connect(**DB)
        cur = db.cursor()
        cur.execute(
            "INSERT INTO orders (customer, product, amount) VALUES (%s, %s, %s)",
            (customer, product, amount),
        )
        db.commit()
        order_id = cur.lastrowid
        cur.close(); db.close()

        # 2. Kafka (ключ = order_id)
        event = {"order_id": order_id, "customer": customer,
                 "product": product, "amount": amount}
        md = get_producer().send(TOPIC, key=order_id, value=event).get(timeout=10)
        log.info("Kafka: order %s -> partition %s", order_id, md.partition)

        # 3. RabbitMQ
        notify(f"Новый заказ №{order_id}: {customer}, {product}, {amount}")

        return jsonify(order_id=order_id, status="created"), 201
    except Exception as e:
        log.exception("Ошибка обработки заказа")
        return jsonify(error=f"Внутренняя ошибка: {e}"), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)