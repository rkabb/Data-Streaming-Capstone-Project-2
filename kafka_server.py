import producer_server

BROKER_URL = "localhost:9092"
TOPIC_NAME = "com.udacity.projects.sfcrime"


def run_kafka_server():

    input_file = "police-department-calls-for-service.json"

    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic=TOPIC_NAME,
        bootstrap_servers=BROKER_URL,
        client_id=f"{TOPIC_NAME}_producer"
    )

    return producer

if __name__ == "__main__":
    producer = run_kafka_server()
    producer.generate_data()