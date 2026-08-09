from pyflink.datastream.functions import ProcessFunction
from utils.haversine import haversine


class ActiveTaxiFilter(ProcessFunction):
    def __init__(self, logger):
        self.logger = logger

    def process_element(self, value, ctx: "ProcessFunction.Context"):
        try:
            taxi_id = value["taxi_id"]
            timestamp = value["timestamp"]
            longitude = value["longitude"]
            latitude = value["latitude"]

            forbidden_city_cords = (39.9163447, 116.3971556)
            distance_to_forbidden_city = haversine(
                latitude, longitude, *forbidden_city_cords
            )

            if distance_to_forbidden_city > 15:
                return

            is_violated = 1 if 10 < distance_to_forbidden_city <= 15 else 0

            active_taxi_data = {
                "taxi_id": taxi_id,
                "timestamp": timestamp,
                "longitude": longitude,
                "latitude": latitude,
                "distance_to_forbidden_city": distance_to_forbidden_city,
                "is_violated": is_violated,
            }

            yield active_taxi_data

        except Exception as e:
            self.logger.log_error(
                f"Error in active taxi calculation for {value[0]}: {e}"
            )
