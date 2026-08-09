# Use an official Python runtime as a parent image
FROM python:3.8-slim-buster

# Set the working directory in the container to /kafka_producer
WORKDIR /kafka_producer

# Add the current directory contents into the container at /kafka_producer
ADD . /kafka_producer

# Install any needed packages specified in requirements.txt
RUN set -ex; \
  	pip install --no-cache-dir -r requirements.txt

# Define environment variable
# ENV NAME World

# Run producer.py when the container launches
CMD ["python", "main.py"]
