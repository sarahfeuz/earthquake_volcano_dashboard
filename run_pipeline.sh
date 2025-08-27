#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    print_info "Checking prerequisites..."
    
    # Check if Docker is installed and running
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
    
    print_status "Docker is running"
    
    # Check if Docker Compose is available
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    print_status "Docker Compose is available"
}

# Start the pipeline
start_pipeline() {
    print_info "Starting complete data pipeline with Delta Lake, Kafka, and Airflow..."
    docker-compose up -d
    
    if [ $? -eq 0 ]; then
        print_status "All services started successfully!"
        print_info "Waiting for services to be ready..."
        
        # Wait for MinIO
        print_info "Waiting for MinIO to be ready..."
        sleep 30
        
        # Wait for Kafka
        print_info "Waiting for Kafka to be ready..."
        sleep 30
        
        # Wait for Airflow
        print_info "Waiting for Airflow to be ready..."
        sleep 60
        
        print_status "Pipeline is ready!"
        print_info "Access points:"
        echo "  - MinIO Console: http://localhost:9001 (minio/minio123)"
        echo "  - Dashboard: http://localhost:8050"
        echo "  - Kafka: localhost:9092"
        echo "  - Spark UI: http://localhost:8080"
        echo "  - Airflow UI: http://localhost:8081 (airflow/airflow)"
    else
        print_error "Failed to start pipeline"
        exit 1
    fi
}

# Stop the pipeline
stop_pipeline() {
    print_info "Stopping data pipeline..."
    docker-compose down
    print_status "Pipeline stopped!"
}

# Restart the pipeline
restart_pipeline() {
    print_info "Restarting data pipeline..."
    docker-compose down
    docker-compose up -d
    print_status "Pipeline restarted!"
}

# Show pipeline status
show_status() {
    print_info "Checking pipeline status..."
    docker-compose ps
    
    print_info "Service logs summary:"
    echo "=== MinIO ==="
    docker-compose logs --tail=5 minio 2>/dev/null || echo "No logs available"
    echo "=== Kafka ==="
    docker-compose logs --tail=5 kafka 2>/dev/null || echo "No logs available"
    echo "=== Streaming Service ==="
    docker-compose logs --tail=5 streaming-service 2>/dev/null || echo "No logs available"
    echo "=== Dashboard ==="
    docker-compose logs --tail=5 visualization 2>/dev/null || echo "No logs available"
    echo "=== Airflow ==="
    docker-compose logs --tail=5 airflow-webserver 2>/dev/null || echo "No logs available"
}

# Show logs for a specific component
show_logs() {
    if [ -z "$1" ]; then
        print_error "Please specify a component to show logs for"
        echo "Available components: minio, kafka, zookeeper, streaming-service, visualization, airflow-webserver, airflow-scheduler"
        exit 1
    fi
    
    print_info "Showing logs for $1..."
    docker-compose logs -f "$1"
}

# Start a specific component
start_component() {
    if [ -z "$1" ]; then
        print_error "Please specify a component to start"
        echo "Available components: minio, kafka, zookeeper, streaming-service, visualization, airflow-webserver, airflow-scheduler"
        exit 1
    fi
    
    print_info "Starting component: $1"
    docker-compose up -d "$1"
}

# Stop a specific component
stop_component() {
    if [ -z "$1" ]; then
        print_error "Please specify a component to stop"
        echo "Available components: minio, kafka, zookeeper, streaming-service, visualization, airflow-webserver, airflow-scheduler"
        exit 1
    fi
    
    print_info "Stopping component: $1"
    docker-compose stop "$1"
}

# Cleanup (remove containers, volumes, networks)
cleanup() {
    print_warning "This will remove all containers, volumes, and networks. Are you sure? (y/N)"
    read -r response
    if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        print_info "Cleaning up..."
        docker-compose down -v --remove-orphans
        print_status "Cleanup completed!"
    else
        print_info "Cleanup cancelled."
    fi
}

# Initialize Airflow
init_airflow() {
    print_info "Initializing Airflow..."
    docker-compose up airflow-init
    print_status "Airflow initialized! Default credentials: airflow/airflow"
}

# Show help
show_help() {
    echo "Data Pipeline Management Script"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  start           Start the complete pipeline"
    echo "  stop            Stop the pipeline"
    echo "  restart         Restart the pipeline"
    echo "  status          Show pipeline status and logs"
    echo "  logs <component> Show logs for a specific component"
    echo "  component <name> Start a specific component"
    echo "  cleanup         Remove all containers, volumes, and networks"
    echo "  init-airflow    Initialize Airflow (first time setup)"
    echo "  help            Show this help message"
    echo ""
    echo "Components:"
    echo "  minio, kafka, zookeeper, streaming-service, visualization"
    echo "  airflow-webserver, airflow-scheduler"
    echo ""
    echo "Access Points:"
    echo "  - MinIO Console: http://localhost:9001 (minio/minio123)"
    echo "  - Dashboard: http://localhost:8050"
    echo "  - Kafka: localhost:9092"
    echo "  - Spark UI: http://localhost:8080"
    echo "  - Airflow UI: http://localhost:8081 (airflow/airflow)"
}

# Main script logic
case "$1" in
    start)
        check_prerequisites
        start_pipeline
        ;;
    stop)
        check_prerequisites
        stop_pipeline
        ;;
    restart)
        check_prerequisites
        restart_pipeline
        ;;
    status)
        check_prerequisites
        show_status
        ;;
    logs)
        check_prerequisites
        show_logs "$2"
        ;;
    component)
        check_prerequisites
        start_component "$2"
        ;;
    cleanup)
        check_prerequisites
        cleanup
        ;;
    init-airflow)
        check_prerequisites
        init_airflow
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        print_error "Unknown command: $1"
        echo ""
        show_help
        exit 1
        ;;
esac 