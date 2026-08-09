from pyflink.common.watermark_strategy import TimestampAssigner
from datetime import datetime


class TaxiTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        timestamp = datetime.strptime(value['timestamp'], "%Y-%m-%d %H:%M:%S")
        return int(timestamp.timestamp() * 1000)
