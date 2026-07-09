import json
import socket
from datetime import datetime, timezone


HOST = "logstash"
PORT = 5044
SERVICES = [
    "splitter",
    "hbase-writer",
    "webhook",
    "pokemon-surname-generator",
    "surname-api",
    "surname-enrichment",
    "hbase-surname-updater",
]


def main():
    timestamp = datetime.now(timezone.utc).isoformat()
    with socket.create_connection((HOST, PORT), timeout=10) as sock:
        for service in SERVICES:
            event = {
                "@timestamp": timestamp,
                "level": "INFO",
                "message": "Logstash seed log for index creation",
                "project": f"rabbitprocessor-pipeline-{service}",
                "serviceName": service,
                "serviceVersion": "Undefined",
                "pipelineName": "Pipeline",
                "requestId": f"logstash-seed-{service}",
                "statusType": "processingSuccess",
            }
            sock.sendall((json.dumps(event) + "\n").encode("utf-8"))


if __name__ == "__main__":
    main()
