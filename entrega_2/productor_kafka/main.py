import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import numpy as np
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_CONSULTAS = os.getenv("TOPIC_CONSULTAS", "consultas")

ZONAS = ["Z1", "Z2", "Z3", "Z4", "Z5"]
CONSULTAS = ["q1", "q2", "q3", "q4", "q5"]

TOTAL_CONSULTAS = int(os.getenv("TOTAL_CONSULTAS", 1000))
DISTRIBUCION = os.getenv("DISTRIBUCION", "zipf")
ZIPF_PARAMETRO = float(os.getenv("ZIPF_PARAMETRO", 1.5))
INTERVALO_SEGUNDOS = float(os.getenv("INTERVALO_SEGUNDOS", 0.1))


def generar_zona_zipf() -> str:
    pesos = np.array([1 / i**ZIPF_PARAMETRO for i in range(1, len(ZONAS) + 1)])
    pesos = pesos / pesos.sum()
    return np.random.choice(ZONAS, p=pesos)


def generar_zona_uniforme() -> str:
    return random.choice(ZONAS)


def generar_zona() -> str:
    if DISTRIBUCION == "zipf":
        return generar_zona_zipf()
    return generar_zona_uniforme()


def generar_consulta() -> dict:
    tipo = random.choice(CONSULTAS)
    confidence_min = round(random.choice([0.0, 0.5, 0.7, 0.9]), 1)
    zona = generar_zona()

    consulta = {
        "request_id": str(uuid.uuid4()),
        "query_type": tipo,
        "zona": zona,
        "params": {},
        "retry_count": 0,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    if tipo in ["q1", "q2", "q3"]:
        consulta["params"] = {
            "confidence_min": confidence_min
        }

    elif tipo == "q4":
        zona_b = random.choice([z for z in ZONAS if z != zona])
        consulta["zona_b"] = zona_b
        consulta["params"] = {
            "confidence_min": confidence_min
        }

    elif tipo == "q5":
        bins = random.choice([5, 10])
        consulta["params"] = {
            "bins": bins
        }

    return consulta


def crear_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
    )


def main():
    print(f"Iniciando Kafka Producer")
    print(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Tópico destino: {TOPIC_CONSULTAS}")
    print(f"Total consultas: {TOTAL_CONSULTAS}")
    print(f"Distribución: {DISTRIBUCION}")

    producer = crear_producer()

    for i in range(TOTAL_CONSULTAS):
        consulta = generar_consulta()

        producer.send(
            TOPIC_CONSULTAS,
            key=consulta["request_id"],
            value=consulta
        )

        if (i + 1) % 100 == 0:
            print(f"Progreso: {i + 1}/{TOTAL_CONSULTAS} consultas publicadas")

        time.sleep(INTERVALO_SEGUNDOS)

    producer.flush()
    producer.close()

    print("Publicación de consultas finalizada")


if __name__ == "__main__":
    main()