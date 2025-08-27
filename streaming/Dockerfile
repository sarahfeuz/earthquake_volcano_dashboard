FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy streaming service and processor
COPY streaming_service.py .
COPY streaming_processor.py .
COPY config/ ./config/

# Make scripts executable
RUN chmod +x streaming_service.py
RUN chmod +x streaming_processor.py

# Default command
CMD ["python3", "streaming_service.py"] 