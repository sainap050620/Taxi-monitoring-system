import os

from data_producer.data_reader import DataReader
from data_producer.engine import Engine
from data_producer.kafka_producer import KafkaProducerWrapper
from utlis.logger import Logger


def driver():
    # Initialize logger
    logger = Logger("logs/logger.log")

    try:

        start_timestamp = os.environ.get("START_TIMESTAMP")
        end_timestamp = os.environ.get("END_TIMESTAMP")
        unprocessed_data_file_path = os.environ.get("UNPROCESSED_DATA_FILE_PATH")
        processed_data_file_path = os.environ.get("PROCESSED_DATA_FILE_PATH")
        fast_forward_speed = int(os.getenv("FAST_FORWARD_SPEED", "60"))
        kafka_topic = os.environ.get("KAFKA_TOPIC")
        kafka_server = os.environ.get("KAFKA_SERVER")

        data_reader = DataReader(
            start_timestamp,
            end_timestamp,
            unprocessed_data_file_path,
            processed_data_file_path,
            logger,
        )

        data = data_reader.get_data()

        print(data.info(memory_usage="deep"))

        producer = KafkaProducerWrapper(
            kafka_topic,
            kafka_server,
            logger,
        )

        engine = Engine(
            data,
            producer,
            start_timestamp,
            end_timestamp,
            fast_forward_speed,
            logger,
        )

        engine.run()

    except Exception as exception:
        logger.log_error(f"An error occurred: {exception}")


if __name__ == "__main__":
    driver()
