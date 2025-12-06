#!/bin/bash
# Vehicle Tracking & Monitoring System - Startup Script

set -e  # Exit on error

echo "Starting Vehicle Monitoring System"
# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Configuration
CONDA_ENV="buat_comvis"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

check_kafka() {
    echo -e "${YELLOW}[2/6] Checking Kafka...${NC}"
    if ! systemctl is-active --quiet kafka 2>/dev/null && ! pgrep -f "kafka.Kafka" > /dev/null; then
        echo -e "${RED}Warning: Kafka is not running!${NC}"
        echo "Starting Kafka with Docker Compose..."
        docker compose -f "${SCRIPT_DIR}/docker-compose.yml" up -d kafka zookeeper
        sleep 5
    fi
    echo -e "${GREEN}[DONE] Kafka is running${NC}"
}

check_influxdb() {
    echo -e "${YELLOW}[3/6] Checking InfluxDB...${NC}"
    if ! systemctl is-active --quiet influxdb 2>/dev/null && ! pgrep -f "influxd" > /dev/null; then
        echo -e "${RED}Warning: InfluxDB is not running!${NC}"
        echo "Starting InfluxDB with Docker Compose..."
        docker compose -f "${SCRIPT_DIR}/docker-compose.yml" up -d influxdb
        sleep 3
    fi
    echo -e "${GREEN}[DONE] InfluxDB is running${NC}"
}

check_grafana() {
    echo -e "${YELLOW}[4/6] Checking Grafana...${NC}"
    if ! systemctl is-active --quiet grafana-server 2>/dev/null && ! pgrep -f "grafana-server" > /dev/null; then
        echo -e "${RED}Warning: Grafana is not running!${NC}"
        echo "Starting Grafana with Docker Compose..."
        docker compose -f "${SCRIPT_DIR}/docker-compose.yml" up -d grafana
        sleep 3
    fi
    echo -e "${GREEN}[DONE] Grafana is running${NC}"
}

start_consumer() {
    echo -e "${YELLOW}[5/6] Starting Kafka Consumer (InfluxDB Writer)...${NC}"
    
    # Activate conda and start consumer in background
    source "$(conda info --base)/etc/profile.d/conda.sh"
    conda activate ${CONDA_ENV}
    
    cd "${SCRIPT_DIR}"
    python kafka_consumer.py > logs/consumer.log 2>&1 &
    CONSUMER_PID=$!
    echo $CONSUMER_PID > .consumer.pid
    echo -e "${GREEN}[DONE] Consumer started (PID: $CONSUMER_PID)${NC}"
}

start_tracking() {
    echo -e "${YELLOW}[6/6] Starting Vehicle Tracking System...${NC}"
    
    source "$(conda info --base)/etc/profile.d/conda.sh"
    conda activate ${CONDA_ENV}
    
    cd "${SCRIPT_DIR}"
    python main_integrated.py
}

cleanup() {
    echo -e "\n${YELLOW}Shutting down...${NC}"
    
    # Stop consumer if running
    if [ -f "${SCRIPT_DIR}/.consumer.pid" ]; then
        CONSUMER_PID=$(cat "${SCRIPT_DIR}/.consumer.pid")
        if ps -p $CONSUMER_PID > /dev/null 2>&1; then
            echo "Stopping Kafka Consumer (PID: $CONSUMER_PID)..."
            kill $CONSUMER_PID
        fi
        rm "${SCRIPT_DIR}/.consumer.pid"
    fi
    
    echo -e "${GREEN}System stopped successfully${NC}"
}

# Main Execution
# Create logs directory if not exists
mkdir -p "${SCRIPT_DIR}/logs"

# Trap Ctrl+C and cleanup
trap cleanup EXIT INT TERM

# Run checks
check_kafka
check_influxdb
check_grafana

# Start services
start_consumer
sleep 2
start_tracking

echo -e "\n${GREEN}=========================================="
echo "System Running Successfully!"
echo "==========================================${NC}"
echo "Grafana Dashboard: http://localhost:3000"
echo "InfluxDB: http://localhost:8086"
echo ""
echo "Press Ctrl+C to stop all services"
