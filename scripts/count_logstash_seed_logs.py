import json
import os
import urllib.request


ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
SERVICES = [
    "splitter",
    "hbase-writer",
    "webhook",
    "pokemon-surname-generator",
    "surname-api",
    "surname-enrichment",
    "hbase-surname-updater",
]


def count_request_id(request_id):
    url = f"{ES_URL}/rabbitprocessor-pipeline-*-2026.07.09/_count"
    body = json.dumps(
        {
            "query": {
                "bool": {
                    "should": [
                        {"term": {"requestId": request_id}},
                        {"term": {"requestId.keyword": request_id}},
                    ],
                    "minimum_should_match": 1,
                }
            }
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))["count"]


def main():
    total = 0
    for service in SERVICES:
        request_id = f"logstash-seed-{service}"
        count = count_request_id(request_id)
        total += count
        print(f"{request_id}: {count}")
    print(f"total: {total}")


if __name__ == "__main__":
    main()
