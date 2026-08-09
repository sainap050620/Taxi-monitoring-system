import json

from utils.haversine import haversine
from utils.redis_helper import RedisHelper

from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types

from datetime import datetime


class CalculateSpeed(KeyedProcessFunction):
    def __init__(self, logger):
        self.last_location_state = None
        self.redis_helper = None
        self.logger = logger

    def open(self, runtime_context: RuntimeContext):
        self.last_location_state = runtime_context.get_state(
            ValueStateDescriptor(
                "last_location",
                Types.TUPLE([Types.FLOAT(), Types.FLOAT(), Types.STRING()]),
            )
        )
        self.redis_helper = RedisHelper()

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        try:
            taxi_id = value["taxi_id"]
            timestamp = value["timestamp"]
            longitude = value["longitude"]
            latitude = value["latitude"]
            current_location = (latitude, longitude)
            current_time = datetime.fromisoformat(timestamp)
            taxi_speed_data = {}

            if current_location == (-1, -1):  # Check if trip has ended
                self.logger.log_info(f"Trip ended for taxi {taxi_id}")
                return

            last_location = self.last_location_state.value()

            if last_location is not None:
                last_lat, last_lon, last_timestamp = last_location
                last_time = datetime.fromisoformat(last_timestamp)
                distance = haversine(
                    last_lat, last_lon, current_location[0], current_location[1]
                )
                time_diff = (current_time - last_time).total_seconds()
                speed = (
                    (distance / time_diff) * 3600 if time_diff > 0 else 0
                )  # speed in km/h

                # Cache speed in Redis
                previous_speeds = self.redis_helper.get(f"{taxi_id}_speeds")
                if previous_speeds:
                    previous_speeds = json.loads(previous_speeds)
                else:
                    previous_speeds = []
                previous_speeds.append(speed)
                self.redis_helper.set(f"{taxi_id}_speeds", json.dumps(previous_speeds))

                is_speeding = 1 if speed > 50 else 0

                taxi_speed_data = {
                    "taxi_id": taxi_id,
                    "timestamp": timestamp,
                    "longitude": longitude,
                    "latitude": latitude,
                    "speed": speed,
                    "is_speeding": is_speeding,
                }

            self.last_location_state.update(
                (value["latitude"], value["longitude"], value["timestamp"])
            )

            if taxi_speed_data:  # Check if taxi_speed_data is not empty
                yield taxi_speed_data
        except Exception as e:
            self.logger.log_error(
                f"Error in speed calculation for {value['taxi_id']}: {e}"
            )
