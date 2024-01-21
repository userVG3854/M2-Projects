import json
from kafka import KafkaConsumer

KAFKA_SERVER = 'localhost:9092'
IN_TOPIC = 'velib-stations'
FILE_PATH = 'velib_stations_archive.txt'

#### we create again a Kafka consumer
consumer = KafkaConsumer(
    IN_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


def archive_data(message):
    with open(FILE_PATH, 'a') as file:
        file.write(json.dumps(message) + '\n')


def main():
    for message in consumer:
        data = message.value
        archive_data(data)
        print(f"Archived data: {data}")


if __name__ == '__main__':
    main()