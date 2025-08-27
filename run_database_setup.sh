#!/bin/bash

echo "=========================================="
echo "DATABASE SETUP"
echo "=========================================="

# Start MinIO
echo "Starting MinIO database..."
docker-compose up -d minio

# Wait for MinIO to be ready
echo "Waiting for MinIO to be ready..."
sleep 10

# Run database initialization
echo "Running database initialization..."
docker-compose up db-init

# Check if initialization was successful
if [ $? -eq 0 ]; then
    echo " Database setup completed successfully!"
    echo ""
    echo "MinIO Console: http://localhost:9001"
    echo "Username: minio"
    echo "Password: minio123"
else
    echo " Database setup failed!"
    exit 1
fi 