import os, time, logging
import pika

logging.basicConfig(level=logging.INFO, format="%(asctime)s [notify] %(message)s")
log = logging.getLogger()
QUEUE = "order-notifications"

def on_message(ch, method, props, body):
    log.info("Уведомление: %s", body.decode())
    ch.basic_ack(delivery_tag=method.delivery_tag)

while True:
    try:
        params = pika.ConnectionParameters(
            host=os.getenv("RABBIT_HOST", "rabbitmq"),
            credentials=pika.PlainCredentials(
                os.getenv("RABBIT_USER", "app"), os.getenv("RABBIT_PASS", "app")),
            heartbeat=60,
        )
        conn = pika.BlockingConnection(params)
        ch = conn.channel()
        ch.queue_declare(queue=QUEUE, durable=True)
        ch.basic_qos(prefetch_count=1)
        ch.basic_consume(queue=QUEUE, on_message_callback=on_message)
        log.info("Воркер уведомлений запущен, очередь %s", QUEUE)
        ch.start_consuming()
    except pika.exceptions.AMQPConnectionError:
        log.warning("RabbitMQ недоступен, повтор через 3 с")
        time.sleep(3)