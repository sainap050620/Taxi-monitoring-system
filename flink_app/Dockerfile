# Base image with Java 11
FROM openjdk:11

# Install Python, pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python

# Set the working directory
WORKDIR /flink_app

# Copy the application files
COPY . /flink_app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Expose the Flink UI port
EXPOSE 8081

# Run main.py when the container launches
CMD ["python", "main.py"]
