import time
import json
import requests
from kafka import KafkaProducer

#### We use here our generated key in the url to have access to the data
API_URL = " https://api.jcdecaux.com/vls/v1/stations?apiKey=87f9a52c024f622961c63d4f10a2bd6bdf03982b"
TOPIC_NAME = "velib-stations"
KAFKA_SERVER = "localhost:9092"

#### Setting our interval time for 10 seconds
POLL_INTERVAL = 10

#### We create a Kafka producer
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


def fetch_data():
    response = requests.get(API_URL)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch data")
        return None


def main():
    while True:
        data = fetch_data()
        if data:
            producer.send(TOPIC_NAME, data)
            print(f"Data sent to topic {TOPIC_NAME}")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()