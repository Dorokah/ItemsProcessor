import json
import os
import sys
from confluent_kafka import Producer


def main():
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    topic = os.environ.get('CONSUME_TOPIC', 'pokedex-raw')
    pokedex_path = os.environ.get('POKEDEX_FILE_PATH', 'pokedex.json')

    print(f"Reading pokedex file from: {pokedex_path}")
    if not os.path.exists(pokedex_path):
        print(f"Error: file not found at {pokedex_path}")
        sys.exit(1)

    with open(pokedex_path, 'r', encoding='utf-8-sig') as f:
        pokedex_content = f.read()

    # Validate that it is valid JSON
    try:
        json.loads(pokedex_content)
    except Exception as e:
        print(f"Error: Invalid JSON format: {e}")
        sys.exit(1)

    print(f"Connecting to Kafka bootstrap servers: {bootstrap_servers}")
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'message.max.bytes': 10000000
    }
    producer = Producer(conf)

    def delivery_report(err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    print(f"Publishing Pokedex content to topic: {topic}")
    producer.produce(
        topic=topic,
        value=pokedex_content.encode('utf-8'),
        callback=delivery_report
    )

    print("Waiting for delivery callback...")
    producer.flush()
    print("Publishing complete!")


if __name__ == '__main__':
    main()
