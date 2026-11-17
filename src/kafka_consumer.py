import os
import json
import certifi
from dotenv import load_dotenv
from confluent_kafka import Consumer

# cargo las variables del .env
load_dotenv()

TOPIC = "etl.records"


def crear_consumer():
    """armo el consumer con los datos de conexión del broker"""
    conf = {
        "bootstrap.servers": "kafka-classroom-2026.westus.cloudapp.azure.com:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "classroom-students",
        "sasl.password": os.getenv("KAFKA_PASSWORD"),
        "ssl.ca.location": certifi.where(),
        "group.id": os.getenv("KAFKA_GROUP_ID"),
        "auto.offset.reset": "earliest",   # arranco desde el primer mensaje del topic
        "enable.auto.commit": False,
    }
    return Consumer(conf)


def consumir_topic(topic=TOPIC, max_vacios=5, timeout=5.0):
    """
    leo todos los mensajes del topic y los voy guardando en una lista.
    paro cuando pasan varios intentos seguidos sin recibir nada
    """
    consumer = crear_consumer()
    consumer.subscribe([topic])

    mensajes = []
    vacios = 0

    try:
        while vacios < max_vacios:
            msg = consumer.poll(timeout=timeout)

            if msg is None:          # no llegó nada en este intento
                vacios += 1
                continue

            if msg.error():          # error de Kafka
                print("Error de Kafka:", msg.error())
                vacios += 1
                continue

            vacios = 0               # llegó un mensaje, reinicio el contador

            mensajes.append({
                "partition": msg.partition(),
                "offset": msg.offset(),
                "timestamp": msg.timestamp()[1],
                "key": msg.key().decode("utf-8") if msg.key() else None,
                "value": json.loads(msg.value().decode("utf-8")),
            })
    finally:
        consumer.close()             # cierro siempre la conexión

    return mensajes


if __name__ == "__main__":
    mensajes = consumir_topic()
    print("Mensajes consumidos:", len(mensajes))
    if mensajes:
        print("\nPrimer mensaje:")
        print(json.dumps(mensajes[0], indent=2, default=str))
