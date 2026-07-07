import requests
import json
import time
import sys


def main():
    kibana_url = "http://localhost:5601"
    headers = {
        "kbn-xsrf": "true",
        "Content-Type": "application/json"
    }

    # Wait for Kibana to be ready
    print("Waiting for Kibana to start up...")
    kibana_ready = False
    for i in range(30):
        try:
            r = requests.get(f"{kibana_url}/api/status", timeout=5)
            if r.status_code == 200:
                print("Kibana is ready!")
                kibana_ready = True
                break
        except Exception:
            pass
        time.sleep(2)

    if not kibana_ready:
        print("Error: Kibana did not start up in time. Please check your docker containers.")
        sys.exit(1)

    # Method 1: Data View API (Kibana 8.x / 9.x)
    data_view_url = f"{kibana_url}/api/data_views/data_view"
    data_view_payload = {
        "data_view": {
            "title": "rabbitprocessor-*",
            "name": "RabbitProcessor Logs",
            "timeFieldName": "@timestamp"
        }
    }

    print("Attempting to create Data View via /api/data_views/data_view...")
    try:
        response = requests.post(data_view_url, headers=headers, json=data_view_payload, timeout=10)
        if response.status_code in [200, 201]:
            print("Success! Created Kibana Data View.")
            print(json.dumps(response.json(), indent=2))
            return
        elif response.status_code == 409:
            print("Data View 'rabbitprocessor-*' already exists.")
            return
        else:
            print(f"Data View API returned status code: {response.status_code}")
    except Exception as e:
        print(f"Data View API connection failed: {e}")

    # Method 2: Fallback Saved Objects API (Kibana 7.x)
    saved_objects_url = f"{kibana_url}/api/saved_objects/index-pattern/rabbitprocessor-logs"
    saved_objects_payload = {
        "attributes": {
            "title": "rabbitprocessor-*",
            "timeFieldName": "@timestamp"
        }
    }

    print("Attempting fallback Saved Objects API via /api/saved_objects/index-pattern...")
    try:
        response = requests.post(saved_objects_url, headers=headers, json=saved_objects_payload, timeout=10)
        if response.status_code in [200, 201]:
            print("Success! Created Index Pattern via Saved Objects API.")
            print(json.dumps(response.json(), indent=2))
        elif response.status_code == 409:
            print("Index Pattern 'rabbitprocessor-*' already exists.")
        else:
            print(f"Saved Objects API failed: {response.status_code}")
            print(response.text)
    except Exception as e:
        print(f"Saved Objects API connection failed: {e}")


if __name__ == '__main__':
    main()
