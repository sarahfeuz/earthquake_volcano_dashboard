FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt

# Upgrade pip and setuptools first
RUN pip install --upgrade pip setuptools wheel

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --force-reinstall -r requirements.txt

# Verify critical packages are working
RUN python3 -c "import requests; print('Requests version:', requests.__version__); print('Has get method:', hasattr(requests, 'get'))"
RUN python3 -c "import kafka; print('Kafka version:', kafka.__version__)"
RUN python3 -c "import boto3; print('Boto3 version:', boto3.__version__)"

# Copy application files
COPY streaming_service.py .
COPY streaming_processor.py .
COPY config/ ./config/

# Set proper permissions
RUN chmod +x streaming_service.py streaming_processor.py

# Run the streaming service
CMD ["python3", "streaming_service.py"]