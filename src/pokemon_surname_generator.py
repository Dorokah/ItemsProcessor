import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

import opentracing
from confluent_kafka import Producer

from src.utils.service_logger import ServiceLogger
from src.utils.tracer import init_tracer, inject_trace_headers


SURNAMES = [
    "Oak",
    "Elm",
    "Birch",
    "Rowan",
    "Juniper",
    "Sycamore",
    "Kukui",
    "Magnolia",
    "Willow",
    "Maple",
]


running = True


def stop(_signum, _frame):
    global running
    running = False


def load_pokemon_ids(pokedex_path):
    with open(pokedex_path, "r", encoding="utf-8") as file:
        pokedex = json.load(file)

    if not isinstance(pokedex, list):
        raise ValueError("Pokedex file must contain a JSON list")

    pokemon_ids = sorted({str(pokemon.get("id")) for pokemon in pokedex if pokemon.get("id") is not None})
    if not pokemon_ids:
        raise ValueError("Pokedex file does not contain any Pokemon IDs")

    return pokemon_ids


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    init_tracer()

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("PUBLISH_TOPIC", "pokemon-surnames")
    pokedex_path = os.environ.get("POKEDEX_FILE_PATH", "pokedex.json")
    interval_seconds = int(os.environ.get("SURNAME_GENERATION_INTERVAL_SECONDS", "60"))

    service_logger = ServiceLogger()
    service_logger.log_service_config()
    service_logger.set_logstash_handler()

    pokemon_ids = load_pokemon_ids(pokedex_path)
    producer = Producer({"bootstrap.servers": bootstrap_servers})

    service_logger.info(
        f"Pokemon surname generator started: topic={topic}, "
        f"pokemonIds={len(pokemon_ids)}, intervalSeconds={interval_seconds}"
    )

    while running:
        start_timestamp = time.time()
        pokemon_id = random.choice(pokemon_ids)
        surname = random.choice(SURNAMES)
        event = {
            "eventType": "pokemon-surname-generated",
            "id": pokemon_id,
            "surname": surname,
            "generatedAt": datetime.now(timezone.utc).isoformat(),
        }

        with opentracing.global_tracer().start_active_span("generate_pokemon_surname") as scope:
            span = scope.span
            span.set_tag("pokemon.id", pokemon_id)
            span.set_tag("pokemon.surname", surname)
            span.set_tag("kafka.topic", topic)

            service_logger.reset_aggregated_log()
            service_logger.log_trace_id()
            service_logger.add_field("requestId", f"surname-generate-{pokemon_id}-{int(start_timestamp)}")
            service_logger.add_field("entityId", pokemon_id)
            service_logger.add_field("surname", surname)
            service_logger.add_field("publishTopic", topic)
            service_logger.add_field("generatedAt", event["generatedAt"])

            try:
                producer.produce(
                    topic=topic,
                    key=pokemon_id.encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                    headers=inject_trace_headers(),
                )
                producer.poll(0)
                service_logger.info(f"Generated surname for Pokemon ID {pokemon_id}: {surname}")
                service_logger.log_success_logstash(start_timestamp)
            except Exception as exc:
                span.set_tag("error", True)
                service_logger.log_error(str(exc), start_timestamp)

        for _ in range(interval_seconds):
            if not running:
                break
            time.sleep(1)

    service_logger.info("Flushing surname generator producer...")
    producer.flush()
    service_logger.info("Pokemon surname generator stopped")
    sys.exit(0)


if __name__ == "__main__":
    main()
