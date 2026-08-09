import json

from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types
from datetime import datetime
from utils.redis_helper import RedisHelper


class CalculateAverageSpeed(KeyedProcessFunction):
    def __init__(self, logger):
        self.last_timestamp_state = None
        self.total_time_state = None
        self.total_distance_state = None
        self.redis_helper = None
        self.logger = logger

    def open(self, runtime_context: RuntimeContext):
        self.total_distance_state = runtime_context.get_state(
            ValueStateDescriptor("total_distance", Types.FLOAT())
        )
        self.total_time_state = runtime_context.get_state(
            ValueStateDescriptor("total_time", Types.FLOAT())
        )
        self.last_timestamp_state = runtime_context.get_state(
            ValueStateDescriptor("last_timestamp", Types.STRING())
        )
        self.redis_helper = RedisHelper()

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        current_time = datetime.fromisoformat(value["timestamp"])
        taxi_id = value["taxi_id"]

        if (
            value["latitude"] == -1 and value["longitude"] == -1
        ):  # Check if trip has ended
            self.logger.log_info(f"Trip ended for taxi {taxi_id}")
            return

        total_distance = self.total_distance_state.value() or 0.0
        total_time = self.total_time_state.value() or 0.0
        last_timestamp = self.last_timestamp_state.value()

        if last_timestamp is not None:
            last_time = datetime.fromisoformat(last_timestamp)
            time_diff = (current_time - last_time).total_seconds()
            total_time += time_diff

        # Cache total time and distance in Redis
        self.redis_helper.set(f"{taxi_id}_total_time", total_time)
        self.redis_helper.set(f"{taxi_id}_total_distance", total_distance)

        # Retrieve cached speeds and calculate average
        cached_speeds = self.redis_helper.get(f"{taxi_id}_speeds")
        if cached_speeds:
            cached_speeds = json.loads(cached_speeds)
            average_speed = sum(cached_speeds) / len(
                cached_speeds
            )  # average speed in km/h
        else:
            average_speed = 0

        # Cache average speed in Redis
        self.redis_helper.set(f"{taxi_id}_average_speed", average_speed)

        avg_speed_data = {"taxi_id": taxi_id, "average_speed": average_speed}

        yield avg_speed_data

        self.total_distance_state.update(total_distance)
        self.total_time_state.update(total_time)
        self.last_timestamp_state.update(value["timestamp"])
