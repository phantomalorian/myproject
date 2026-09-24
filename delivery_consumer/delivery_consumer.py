import os, json, time, logging
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [delivery] %(message)s")
log = logging.getLogger()

def connect():
    while True:
        try:
            return KafkaConsumer(
                "order-events",
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
                group_id="delivery-group",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                key_deserializer=lambda k: k.decode(),
                value_deserializer=lambda v: json.loads(v.decode()),
            )
        except NoBrokersAvailable:
            log.warning("Kafka недоступна, повтор через 3 с")
            time.sleep(3)

consumer = connect()
log.info("Консьюмер доставки запущен, группа delivery-group")
for msg in consumer:
    o = msg.value
    log.info("partition=%s offset=%s key=%s | Подготовка доставки заказа #%s (%s) для %s",
             msg.partition, msg.offset, msg.key, o["order_id"], o["product"], o["customer"])
    time.sleep(1) 
    log.info("Доставка заказа #%s подготовлена", o["order_id"])