import json
from kafka import KafkaConsumer, KafkaProducer

KAFKA_SERVER = 'localhost:9092'
IN_TOPIC = 'velib-stations'
OUT_TOPIC = 'stations-status'

#### We create  a Kafka consumer and producer
consumer = KafkaConsumer(
    IN_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

#### dict for keeping track of the last known state of each station
last_known_state = {}


def process_station_data(station):
    station_id = station['number']
    current_state = (station['available_bike_stands'],
                     station['available_bikes'])

    #### We check if there is a change
    if station_id not in last_known_state or last_known_state[station_id]!= current_state:
        last_known_state[station_id] = current_state
        producer.send(OUT_TOPIC, station)
        print(f"Updated data sent for station {station_id}")


def main():
    for message in consumer:
        data = message.value
        for station in data:
            process_station_data(station)


if __name__ == '__main__':
    main()
