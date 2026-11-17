import os
import json
import certifi
from dotenv import load_dotenv
from confluent_kafka import Consumer

# Carga las variables del archivo .env (contraseña y group.id)
load_dotenv()

TOPIC = "etl.records"


def crear_consumer():
    """Crea el consumer """
    conf = {
        "bootstrap.servers": "kafka-classroom-2026.westus.cloudapp.azure.com:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "classroom-students",
        "sasl.password": os.getenv("KAFKA_PASSWORD"),
        "ssl.ca.location": certifi.where(),
        "group.id": os.getenv("KAFKA_GROUP_ID"),
        "auto.offset.reset": "earliest",   # empezar desde el primer mensaje del topic
        "enable.auto.commit": False,
    }
    return Consumer(conf)


def consumir_topic(topic=TOPIC, max_vacios=5, timeout=5.0):
    """
    Lee todos los mensajes del topic y los acumula en una lista (estado en memoria).
    Se detiene cuando pasan `max_vacios` intentos seguidos sin recibir mensajes.
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

            vacios = 0               # llegó un mensaje: reiniciamos el contador

            mensajes.append({
                "partition": msg.partition(),
                "offset": msg.offset(),
                "timestamp": msg.timestamp()[1],
                "key": msg.key().decode("utf-8") if msg.key() else None,
                "value": json.loads(msg.value().decode("utf-8")),
            })
    finally:
        consumer.close()             # siempre cerramos la conexion

    return mensajes


if __name__ == "__main__":
    mensajes = consumir_topic()
    print("Mensajes consumidos:", len(mensajes))
    if mensajes:
        print("\nPrimer mensaje:")
        print(json.dumps(mensajes[0], indent=2, default=str))