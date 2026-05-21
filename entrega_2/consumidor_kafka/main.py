import json
import os
import time

import httpx
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

TOPIC_CONSULTAS = os.getenv("TOPIC_CONSULTAS", "consultas")
TOPIC_RETRY = os.getenv("TOPIC_RETRY", "consultas_retry")

CACHE_URL = os.getenv("CACHE_URL", "http://servicio_cache:8002")

CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "grupo_consultas")


def construir_endpoint(mensaje: dict) -> str:
    tipo = mensaje["query_type"]
    zona = mensaje["zona"]
    params = mensaje.get("params", {})

    if tipo == "q1":
        return f"/consulta/q1/{zona}?confidence_min={params['confidence_min']}"

    elif tipo == "q2":
        return f"/consulta/q2/{zona}?confidence_min={params['confidence_min']}"

    elif tipo == "q3":
        return f"/consulta/q3/{zona}?confidence_min={params['confidence_min']}"

    elif tipo == "q4":
        zona_b = mensaje["zona_b"]
        return f"/consulta/q4/{zona}/{zona_b}?confidence_min={params['confidence_min']}"

    elif tipo == "q5":
        return f"/consulta/q5/{zona}?bins={params['bins']}"

    raise Exception(f"Tipo de consulta desconocido: {tipo}")


def crear_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_CONSULTAS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )


def procesar_consulta(cliente: httpx.Client, mensaje: dict):
    endpoint = construir_endpoint(mensaje)

    url = f"{CACHE_URL}{endpoint}"

    inicio = time.time()

    respuesta = cliente.get(url, timeout=30)

    latencia = (time.time() - inicio) * 1000

    print("=" * 60)
    print(f"Consulta procesada")
    print(f"Request ID: {mensaje['request_id']}")
    print(f"Tipo: {mensaje['query_type']}")
    print(f"Zona: {mensaje['zona']}")
    print(f"Status HTTP: {respuesta.status_code}")
    print(f"Latencia: {latencia:.2f} ms")

    if respuesta.status_code == 200:
        datos = respuesta.json()

        print(f"Cache hit: {datos.get('cache_hit', False)}")

    print("=" * 60)


def main():
    print("Iniciando consumidor Kafka")

    consumer = crear_consumer()

    with httpx.Client() as cliente:
        for mensaje in consumer:

            consulta = mensaje.value

            try:
                procesar_consulta(cliente, consulta)

            except Exception as e:
                print(f"Error procesando consulta: {e}")


if __name__ == "__main__":
    main()