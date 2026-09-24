import os, json, time, logging
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [payment] %(message)s")
log = logging.getLogger()

def connect():
    while True:
        try:
            return KafkaConsumer(
                "order-events",
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
                group_id="payment-group",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                key_deserializer=lambda k: k.decode(),
                value_deserializer=lambda v: json.loads(v.decode()),
            )
        except NoBrokersAvailable:
            log.warning("Kafka недоступна, повтор через 3 с")
            time.sleep(3)

consumer = connect()
log.info("Консьюмер оплаты запущен, группа payment-group")
for msg in consumer:
    o = msg.value
    log.info("partition=%s offset=%s key=%s | Обработка оплаты заказа #%s на сумму %s",
             msg.partition, msg.offset, msg.key, o["order_id"], o["amount"])
    time.sleep(1)  # имитация обработки
    log.info("Оплата заказа #%s прошла успешно", o["order_id"])