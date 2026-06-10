FROM python:3.11-slim

WORKDIR /app

# Install confluent-kafka — 3.11 has pre-built wheels, no compilation needed
RUN pip install --no-cache-dir confluent-kafka==2.4.0

# Copy both scripts into the image
COPY create_topics.py .
COPY producer.py .

# Default entrypoint — override by passing script name as argument
ENTRYPOINT ["python"]
