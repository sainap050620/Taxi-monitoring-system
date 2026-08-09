import json

from confluent_kafka import SerializingProducer


class KafkaProducerWrapper:
    def __init__(self, kafka_topic, kafka_server, logger):
        self.logger = logger

        # Load Kafka configuration
        self.kafka_topic = kafka_topic
        self.kafka_server = kafka_server
        self.producer = self.create_producer()

    def create_producer(self):
        try:
            producer = SerializingProducer({"bootstrap.servers": self.kafka_server})
            return producer
        except Exception as exception:
            self.logger.log_error(
                f"An error occurred while creating the producer: {exception}"
            )
            return None

    def produce_message(self, key, data):
        try:
            self.producer.produce(
                self.kafka_topic,
                key=str(key),
                value=json.dumps(data),
            )
            self.producer.poll(0)
        except Exception as exception:
            self.logger.log_error(
                f"An error occurred while producing the message: {exception}"
            )

    def close(self):
        # Wait for all messages to be delivered
        self.producer.flush()
