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

# Copy visualization application
COPY dashboard.py .
COPY config/ ./config/

# Make script executable
RUN chmod +x dashboard.py

# Expose port for Dash server
EXPOSE 8050

# Default command
CMD ["python3", "dashboard.py"] 