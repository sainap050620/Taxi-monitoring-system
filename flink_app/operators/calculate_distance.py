from utils.haversine import haversine
from utils.redis_helper import RedisHelper

from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, ReducingStateDescriptor
from pyflink.common.typeinfo import Types


class CalculateDistance(KeyedProcessFunction):
    def __init__(self, logger):
        self.distance_state = None
        self.last_location_state = None
        self.total_distance_state = None
        self.redis_helper = None
        self.logger = logger

    def open(self, runtime_context: RuntimeContext):
        self.last_location_state = runtime_context.get_state(
            ValueStateDescriptor(
                "last_location", Types.TUPLE([Types.FLOAT(), Types.FLOAT()])
            )
        )
        self.distance_state = runtime_context.get_state(
            ValueStateDescriptor("individual_total_distance", Types.FLOAT())
        )
        self.total_distance_state = runtime_context.get_reducing_state(
            ReducingStateDescriptor("total_distance", lambda x, y: x + y, Types.FLOAT())
        )
        self.redis_helper = RedisHelper()

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        try:
            current_location = (value["latitude"], value["longitude"])
            taxi_id = value["taxi_id"]

            if current_location == (-1, -1):  # Check if trip has ended
                self.logger.log_info(f"Trip ended for taxi {taxi_id}")
                return

            individual_total_distance = self.distance_state.value() or 0.0
            last_location = self.last_location_state.value()
            total_distance = 0.0  # Initialize total_distance

            if last_location is not None:
                last_lat, last_lon = last_location
                distance = haversine(
                    last_lat, last_lon, current_location[0], current_location[1]
                )
                individual_total_distance += distance

                # Cache total distance in Redis
                self.redis_helper.set(
                    f"{taxi_id}_total_distance", individual_total_distance
                )

                # Update total distance travelled by all taxis in Redis
                total_distance = self.redis_helper.get("total_distance")
                if total_distance is None:
                    total_distance = 0.0
                else:
                    total_distance = float(total_distance)
                total_distance += distance
                self.redis_helper.set("total_distance", total_distance)

                # Update total distance state
                self.total_distance_state.add(distance)

            self.distance_state.update(individual_total_distance)
            self.last_location_state.update((value["latitude"], value["longitude"]))

            if last_location is not None:  # Yield overall_distance only if updated
                overall_distance = {"total_distance": total_distance}
                yield overall_distance

        except Exception as e:
            self.logger.log_error(
                f"Error in distance calculation for {value['taxi_id']}: {e}"
            )
