"""
Kafka Consumer Module
Consumes vehicle data from Kafka and writes to InfluxDB
"""

from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import json
import logging
from datetime import datetime

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class VehicleDataConsumer:
    def __init__(
        self,
        kafka_servers=['localhost:9092'],
        kafka_topic='vehicle-data',
        influx_url='http://localhost:8086',
        influx_token='your-token-here',  # Change this!
        influx_org='vehicle-monitoring',
        influx_bucket='vehicle-counts'
    ):
        """
        Initialize Kafka Consumer and InfluxDB Client
        
        Args:
            kafka_servers: List of Kafka broker addresses
            kafka_topic: Kafka topic to consume from
            influx_url: InfluxDB server URL
            influx_token: InfluxDB authentication token
            influx_org: InfluxDB organization name
            influx_bucket: InfluxDB bucket name
        """
        # Kafka Consumer
        self.consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=kafka_servers,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='vehicle-monitoring-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # InfluxDB Client
        self.influx_client = InfluxDBClient(
            url=influx_url,
            token=influx_token,
            org=influx_org
        )
        self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
        self.bucket = influx_bucket
        self.org = influx_org
        
        logging.info(f"Consumer initialized. Kafka topic: {kafka_topic}")
        logging.info(f"InfluxDB bucket: {influx_bucket}")
    
    def write_to_influxdb(self, data):
        """
        Write vehicle count data to InfluxDB
        
        Args:
            data: Dictionary containing vehicle count data
        """
        try:
            timestamp = datetime.fromisoformat(data['timestamp'])
            location = data['location']
            counts = data['counts']
            total = data['total']
            
            # Create point for total count
            point = Point("vehicle_count") \
                .tag("location", location) \
                .field("total", total) \
                .field("car", counts.get('car', 0)) \
                .field("motorcycle", counts.get('motorcycle', 0)) \
                .field("bus", counts.get('bus', 0)) \
                .field("truck", counts.get('truck', 0)) \
                .time(timestamp)
            
            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            logging.info(f"Written to InfluxDB: {location} - Total: {total}")
            
        except Exception as e:
            logging.error(f"Failed to write to InfluxDB: {e}")
    
    def start_consuming(self):
        """Start consuming messages from Kafka"""
        logging.info("Starting to consume messages...")
        
        try:
            for message in self.consumer:
                data = message.value
                logging.debug(f"Received: {data}")
                self.write_to_influxdb(data)
                
        except KeyboardInterrupt:
            logging.info("Stopping consumer...")
        finally:
            self.close()
    
    def close(self):
        """Close connections"""
        self.consumer.close()
        self.influx_client.close()
        logging.info("Consumer closed")


if __name__ == "__main__":
    # Start consumer
    consumer = VehicleDataConsumer()
    consumer.start_consuming()
