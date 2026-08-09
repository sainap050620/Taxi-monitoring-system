from datetime import datetime, timedelta
from time import sleep


class Engine:
    def __init__(
        self, data, producer, start_timestamp, end_timestamp, fast_forward_speed, logger
    ):
        self.data = data
        self.producer = producer
        self.start_timestamp = datetime.strptime(start_timestamp, "%Y-%m-%d %H:%M:%S")
        self.end_timestamp = datetime.strptime(end_timestamp, "%Y-%m-%d %H:%M:%S")
        self.fast_forward_speed = fast_forward_speed
        self.logger = logger

    def feed_data_to_producer(self, start_time, end_time):
        try:
            mask = (self.data["timestamp"] >= start_time) & (
                self.data["timestamp"] < end_time
            )
            selected_data = self.data.loc[mask]

            for _, row in selected_data.iterrows():
                try:
                    self.producer.produce_message(
                        key=row["taxi_id"],
                        data={
                            "taxi_id": row["taxi_id"],
                            "timestamp": row["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                            "longitude": row["longitude"],
                            "latitude": row["latitude"],
                        },
                    )
                except BufferError:
                    self.logger.log_error("Queue is full, waiting...")
                    sleep(1)
                    continue

        except Exception as e:
            self.logger.log_error(
                f"An error occurred while putting data to producer: {e}"
            )

    def run(self):
        try:
            current_timestamp = self.start_timestamp

            while current_timestamp <= self.end_timestamp:
                sleep(1)
                next_timestamp = current_timestamp + timedelta(
                    seconds=self.fast_forward_speed
                )
                self.feed_data_to_producer(current_timestamp, next_timestamp)
                current_timestamp = next_timestamp

        except Exception as e:
            self.logger.log_error(f"An error occurred while running the engine: {e}")
        finally:
            self.producer.close()
