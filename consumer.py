from kafka import KafkaConsumer

if __name__ == '__main__':
    topic = 'com.udacity.projects.sfcrime'
    consumer = KafkaConsumer(topic, auto_offset_reset='earliest', bootstrap_servers=['localhost:9092'])
    for msg in consumer:
        print(msg.value)

    if consumer is not None:
        consumer.close()