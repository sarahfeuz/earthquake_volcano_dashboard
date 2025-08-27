#!/bin/bash

echo "=========================================="
echo " PIPELINE STATUS CHECK"
echo "=========================================="

# Check if containers are running
echo " Container Status:"
if docker ps | grep -q "minio-db"; then
    echo "   MinIO Database: Running"
else
    echo "   MinIO Database: Not running"
fi

if docker ps | grep -q "streaming-service"; then
    echo "   Earthquake Streaming Service: Running"
else
    echo "   Earthquake Streaming Service: Not running"
fi

if docker ps | grep -q "streaming-processor"; then
    echo "   Earthquake Processor: Running"
else
    echo "   Earthquake Processor: Not running"
fi

if docker ps | grep -q "visualization-dashboard"; then
    echo "   Visualization Dashboard: Running"
else
    echo "   Visualization Dashboard: Not running"
fi

echo ""
echo " Access Points:"
echo "   Dashboard: http://localhost:8050"
echo "   MinIO Console: http://localhost:9001 (minio/minio123)"

echo ""
echo " Data Sources:"
echo "   Earthquake Data: USGS API (updates every 60s)"
echo "   World Bank Data: Economic indicators for 30 countries"
echo "   Processing: Real-time earthquake processing"

echo ""
echo " Recent Activity:"
echo "   Earthquake Ingestion: $(docker logs streaming-service 2>/dev/null | grep "Uploaded.*earthquakes" | tail -1 | cut -d' ' -f6- || echo "No recent activity")"
echo "   Earthquake Processing: $(docker logs streaming-processor 2>/dev/null | grep "Uploaded.*processed" | tail -1 | cut -d' ' -f6- || echo "No recent activity")"

echo ""
echo " Management Commands:"
echo "  Stop all: docker-compose down"
echo "  View logs: docker-compose logs [service-name]"
echo "  Restart streaming: docker-compose restart streaming-service streaming-processor"
echo "  Restart dashboard: docker-compose restart visualization"
echo "  Run full pipeline: ./run_pipeline.sh" 