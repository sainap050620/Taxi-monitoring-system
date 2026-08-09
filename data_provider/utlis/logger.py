import logging
import os


class Logger:
    def __init__(self, log_file):
        self.log_file = log_file

        # Create the logs directory if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.ERROR)  # Set the logging level to ERROR

        # Create a file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.ERROR)

        # Create a logging format
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s - %(filename)s - Line: %(lineno)d - Traceback: %(exc_info)s')

        file_handler.setFormatter(formatter)

        # Add the file handler to the logger
        self.logger.addHandler(file_handler)

    def log_error(self, message):
        self.logger.error(message)
