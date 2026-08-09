from data_consumer.stream_processing import StreamProcessing
from utils.logger import Logger

if __name__ == "__main__":
    logger = Logger("logs/logger.log")
    try:
        streaming_process = StreamProcessing(logger)
        streaming_process.execute()
    except Exception as exception:
        logger.log_error(exception)
