import json
from kafka import KafkaConsumer, KafkaProducer

KAFKA_SERVER = 'localhost:9092'
IN_TOPIC = 'stations-status'
OUT_TOPIC = 'empty-stations'

#### We create a Kafka consumer and producer
consumer = KafkaConsumer(
    IN_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

#### our dict to keep track of the empty status of each station
empty_stations = {}


def process_station_data(station):
    station_id = station['number']
    is_empty = station['available_bikes'] == 0

    #### we check for status change
    if station_id not in empty_stations or empty_stations[station_id] != is_empty:
        empty_stations[station_id] = is_empty
        status_change = 'became empty' if is_empty else 'is no longer empty'
        message = f"Station {station_id} {status_change}"
        producer.send(OUT_TOPIC, {'station_id': station_id, 'message': message})
        print(message)


def main():
    for message in consumer:
        data = message.value
        process_station_data(data)


if __name__ == '__main__':
    main()
