import os
import shutil
import zipfile
from datetime import datetime

import pandas as pd


class DataReader:
    def __init__(
        self,
        start_timestamp,
        end_timestamp,
        unprocessed_data_file_path,
        processed_data_file_path,
        logger,
    ):
        self.logger = logger
        self.unprocessed_data_file_path = unprocessed_data_file_path
        self.processed_data_file_path = processed_data_file_path
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp

    def process_line(self, line):
        try:
            # Split the line into components
            components = line.split(",")

            # Assign each component to a variable
            taxi_id, timestamp, longitude, latitude = components

            # Create a dictionary with the data
            return {
                "taxi_id": int(taxi_id),
                "timestamp": datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S"),
                "longitude": float(longitude),
                "latitude": float(latitude),
            }
        except Exception as exception:
            self.logger.log_error(
                f"An error occurred while processing line: {exception}"
            )
            return None

    def initialize_taxi_data_files(self):
        try:
            # Extract the zip file
            with zipfile.ZipFile(self.unprocessed_data_file_path, "r") as zip_ref:
                self.extracted_unprocessed_data_file_path = os.path.splitext(
                    self.unprocessed_data_file_path
                )[0]
                zip_ref.extractall(self.extracted_unprocessed_data_file_path)

            # Get all text files in the extracted directory
            taxi_data_file_paths = [
                os.path.join(self.extracted_unprocessed_data_file_path, file_name)
                for file_name in os.listdir(self.extracted_unprocessed_data_file_path)
                if os.path.isfile(
                    os.path.join(self.extracted_unprocessed_data_file_path, file_name)
                )
                and file_name.endswith(".txt")
            ]

            return taxi_data_file_paths
        except FileNotFoundError as e:
            self.logger.log_error(f"File not found error: {e}")
            return None
        except Exception as e:
            self.logger.log_error(
                f"An error occurred while initializing taxi data files: {e}"
            )
            return None

    def read_and_process_data(self, file_path):
        try:
            with open(file_path, "r") as file:
                lines = file.readlines()

            # Check if the file is empty
            if not lines:
                return None

            data_list = []
            for line in lines:
                data = self.process_line(line)
                if data is not None:
                    data_list.append(data)

            data_list.append(
                {
                    "taxi_id": data_list[-1]["taxi_id"],
                    "timestamp": data_list[-1]["timestamp"],
                    "longitude": -1,
                    "latitude": -1,
                }
            )

            return pd.DataFrame(data_list)

        except FileNotFoundError as e:
            self.logger.log_error(f"File not found error: {e}")
            return None
        except Exception as e:
            self.logger.log_error(
                f"An error occurred while reading and processing data: {e}"
            )
            return None

    def process_all_files(self):
        try:
            taxi_data_file_paths = self.initialize_taxi_data_files()

            df_list = []
            total_files = len(taxi_data_file_paths)
            for i, file_path in enumerate(taxi_data_file_paths):
                percentage = (i + 1) / total_files * 100
                print(
                    f"\rProcessing file {i+1} of {total_files} ({percentage:.2f}%)",
                    end="",
                )

                df = self.read_and_process_data(file_path)
                if df is not None:
                    df_list.append(df)

            final_df = pd.concat(df_list, ignore_index=True).sort_values("timestamp")
            final_df.to_pickle(self.processed_data_file_path)

            # Delete the extracted_unprocessed_data_file_path directory
            shutil.rmtree(self.extracted_unprocessed_data_file_path)

            return final_df

        except Exception as e:
            self.logger.log_error(f"An error occurred while processing all files: {e}")
            return None

    def get_data(self):
        try:
            if os.path.exists(self.processed_data_file_path):
                return pd.read_pickle(self.processed_data_file_path)
            else:
                return self.process_all_files()
        except Exception as e:
            self.logger.log_error(f"An error occurred while getting data: {e}")
            return None
