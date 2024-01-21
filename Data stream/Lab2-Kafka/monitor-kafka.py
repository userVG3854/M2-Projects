from kafka import KafkaConsumer, TopicPartition
import time

KAFKA_SERVER = 'localhost:9092'

consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER)

def get_topic_status():
    for topic in consumer.topics():
        partitions = consumer.partitions_for_topic(topic)
        if partitions:
            for partition in partitions:
                tp = TopicPartition(topic, partition)

                #### we create a new consumer instance to fetch offsets for each partition
                partition_consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER)
                partition_consumer.assign([tp])
                partition_consumer.seek_to_end(tp)
                last_offset = partition_consumer.position(tp)
                print(f"Topic: {topic}, Partition: {partition}, Offset: {last_offset}, Timestamp: {int(time.time() * 1000)}")
                partition_consumer.close()

def main():
    while True:
        get_topic_status()
        time.sleep(20)  

if __name__ == '__main__':
    main()