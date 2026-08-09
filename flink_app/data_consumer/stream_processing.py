import json
import redis

from operators.active_taxi import ActiveTaxiFilter
from operators.calculate_avg_speed import CalculateAverageSpeed
from operators.calculate_distance import CalculateDistance
from operators.calculate_speed import CalculateSpeed
from operators.ended_taxi import EndedTripCounter

from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.common.time import Duration
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream.formats.json import JsonRowDeserializationSchema

from operators.preprocess import PreprocessStream
from utils.time_stamp_assigner import TaxiTimestampAssigner


class StreamProcessing:
    def __init__(self, logger):
        self.logger = logger
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
        self.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        self.env.set_parallelism(1)

        # Adding JAR files to the classpath
        self.env.add_jars(
            "file:///flink_app/dependency/flink-connector-kafka-3.1.0-1.18.jar",
            "file:///flink_app/dependency/kafka-clients-3.7.0.jar",
        )
        # Redis client
        self.redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)

    def execute(self):
        try:
            # Kafka consumer properties
            kafka_properties = {
                "bootstrap.servers": "broker:29092",
                "group.id": "taxi-group",
                "auto.offset.reset": "earliest",
            }

            # Kafka topic(s) to read from
            topics = ["taxi"]

            # Creating Kafka consumer
            deserialization_schema = (
                JsonRowDeserializationSchema.builder()
                .type_info(
                    type_info=Types.ROW_NAMED(
                        ["taxi_id", "timestamp", "longitude", "latitude"],
                        [Types.INT(), Types.STRING(), Types.FLOAT(), Types.FLOAT()],
                    )
                )
                .build()
            )

            kafka_consumer = FlinkKafkaConsumer(
                topics, deserialization_schema, kafka_properties
            )

            # Define Kafka producers

            # Define Kafka producer for active taxis
            kafka_active_taxi_producer = FlinkKafkaProducer(
                topic="active_taxi",
                serialization_schema=SimpleStringSchema(),
                producer_config={"bootstrap.servers": "broker:29092"},
            )

            # Define Kafka producer for metrics
            kafka_ended_taxi_producer = FlinkKafkaProducer(
                topic="ended_taxi",
                serialization_schema=SimpleStringSchema(),
                producer_config={"bootstrap.servers": "broker:29092"},
            )

            # Define Kafka producer for metrics
            kafka_metrics_producer = FlinkKafkaProducer(
                topic="metrics",
                serialization_schema=SimpleStringSchema(),
                producer_config={"bootstrap.servers": "broker:29092"},
            )

            # Define Kafka producer for over_speeding
            kafka_over_speed_producer = FlinkKafkaProducer(
                topic="over_speeding",
                serialization_schema=SimpleStringSchema(),
                producer_config={"bootstrap.servers": "broker:29092"},
            )

            # Define Kafka producer for avg_speed
            kafka_avg_speed_producer = FlinkKafkaProducer(
                topic="avg_speed",
                serialization_schema=SimpleStringSchema(),
                producer_config={"bootstrap.servers": "broker:29092"},
            )

            # Define a watermark strategy
            watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
                Duration.of_seconds(10)
            )
            watermark_strategy = watermark_strategy.with_timestamp_assigner(
                TaxiTimestampAssigner()
            )

            data_stream = self.env.add_source(
                kafka_consumer
            ).assign_timestamps_and_watermarks(watermark_strategy)

            # Apply preprocess_stream to check for duplicates and abnormal speeds
            preprocess_stream = data_stream.key_by(lambda x: x["taxi_id"]).process(
                PreprocessStream(self.logger)
            )

            # Applying stream processing operators
            filtered_stream_string = preprocess_stream.process(
                ActiveTaxiFilter(self.logger)
            ).map(lambda x: json.dumps(x), output_type=Types.STRING())
            filtered_stream = preprocess_stream.process(ActiveTaxiFilter(self.logger))

            # Apply EndedTripCounter operator
            ended_trip_stream = (
                filtered_stream.key_by(lambda x: x["taxi_id"])
                .process(EndedTripCounter(self.logger))
                .map(lambda x: json.dumps(x), output_type=Types.STRING())
            )

            # Apply CalculateDistance operator
            distance_stream = (
                filtered_stream.key_by(lambda x: x["taxi_id"])
                .process(CalculateDistance(self.logger))
                .map(lambda x: json.dumps(x), output_type=Types.STRING())
            )

            # Apply CalculateSpeed operator
            speed_stream = (
                filtered_stream.key_by(lambda x: x["taxi_id"])
                .process(CalculateSpeed(self.logger))
                .map(lambda x: json.dumps(x), output_type=Types.STRING())
            )

            # Apply CalculateAverageSpeed operator
            avg_speed_stream = (
                filtered_stream.key_by(lambda x: x["taxi_id"])
                .process(CalculateAverageSpeed(self.logger))
                .map(lambda x: json.dumps(x), output_type=Types.STRING())
            )

            # Send filtered data to Kafka topic
            filtered_stream_string.add_sink(kafka_active_taxi_producer)
            ended_trip_stream.add_sink(kafka_ended_taxi_producer)
            distance_stream.add_sink(kafka_metrics_producer)
            speed_stream.add_sink(kafka_over_speed_producer)
            avg_speed_stream.add_sink(kafka_avg_speed_producer)

            # Executing Flink job
            self.env.execute("Taxi Monitoring Job")
        except Exception as exception:
            # Log the exception
            self.logger.log_error(f"Error executing Flink job: {exception}")
