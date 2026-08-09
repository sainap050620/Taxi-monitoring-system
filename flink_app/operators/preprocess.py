from datetime import datetime
from utils.haversine import haversine
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types


class PreprocessStream(KeyedProcessFunction):
    def __init__(self, logger):
        self.logger = logger
        self.last_location_state = None

    def open(self, runtime_context: RuntimeContext):
        self.last_location_state = runtime_context.get_state(
            ValueStateDescriptor(
                "last_location",
                Types.TUPLE([Types.FLOAT(), Types.FLOAT(), Types.STRING()]),
            )
        )

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        try:
            taxi_id = value["taxi_id"]
            timestamp = value["timestamp"]
            longitude = value["longitude"]
            latitude = value["latitude"]
            current_location = (latitude, longitude)
            current_time = datetime.fromisoformat(timestamp)

            # Checking for duplicate timestamps
            last_location = self.last_location_state.value()
            if last_location is not None:
                last_lat, last_lon, last_timestamp = last_location
                last_time = datetime.fromisoformat(last_timestamp)

                if current_time == last_time and current_location != (-1, -1):
                    self.logger.log_info(
                        f"Duplicate timestamp for taxi {taxi_id}, ignoring record."
                    )
                    return

                # Calculate speed to check for anomalies
                distance = haversine(
                    last_lat, last_lon, current_location[0], current_location[1]
                )
                time_diff = (current_time - last_time).total_seconds()
                if time_diff > 0:
                    speed = (distance / time_diff) * 3600  # speed in km/h
                    if speed > 300:  # Assuming 300 km/h as a threshold for anomaly
                        self.logger.log_info(
                            f"Anomalous speed detected for taxi {taxi_id}, ignoring record."
                        )
                        return

            # Update last location state
            self.last_location_state.update((latitude, longitude, timestamp))

            yield value
        except Exception as e:
            self.logger.log_error(f"Error in preprocessing for {value['taxi_id']}: {e}")
