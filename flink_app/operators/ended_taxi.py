from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, ValueState
from pyflink.common.typeinfo import Types


class EndedTripCounter(KeyedProcessFunction):
    def __init__(self, logger):
        self.ended_trip_count_state = None
        self.logger = logger

    def open(self, runtime_context: RuntimeContext):
        # Initialize the state to hold the count of ended trips
        ended_trip_count_descriptor = ValueStateDescriptor(
            "ended_trip_count", Types.INT()
        )
        self.ended_trip_count_state = runtime_context.get_state(
            ended_trip_count_descriptor
        )

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        try:
            longitude = value["longitude"]
            latitude = value["latitude"]
            current_count = 0

            # Check if the trip has ended
            if longitude == -1 and latitude == -1:
                current_count = self.ended_trip_count_state.value()
                if current_count is None:
                    current_count = 0
                current_count += 1
                self.ended_trip_count_state.update(current_count)

                # Yield the count of ended trips
                yield {"ended_trip_count": current_count}

        except Exception as e:
            self.logger.log_error(f"Error in end trip calculation for {value}: {e}")
