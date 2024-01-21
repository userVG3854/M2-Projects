import json
from kafka import KafkaConsumer

KAFKA_SERVER = 'localhost:9092'
IN_TOPIC = 'empty-stations'

#### we start by creating a kafka consumer
consumer = KafkaConsumer(
    IN_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

#### function for the message

def print_alert(message):
    station_info = message.get('station_info', {})
    address = station_info.get('address', 'Unknown address')
    city = station_info.get('contract_name', 'Unknown city')
    if station_info.get('available_bikes', 1) == 0:
        print(f"Alert: There is a station at {address} in {city} which is now empty !!!")


def main():
    for message in consumer:
        data = message.value
        print_alert(data)


if __name__ == '__main__':
    main()