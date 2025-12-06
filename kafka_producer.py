"""
Kafka Producer Module
Sends vehicle counting data to Kafka topic
"""

from kafka import KafkaProducer
import json
import logging
from datetime import datetime

class VehicleDataProducer:
    def __init__(self, kafka_servers=['localhost:9092'], topic='vehicle-data'):
        """
        Initialize Kafka Producer
        
        Args:
            kafka_servers: List of Kafka broker addresses
            topic: Kafka topic name
        """
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  # Wait for all replicas
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        logging.info(f"Kafka Producer initialized. Topic: {topic}")
    
    def send_count_data(self, vehicle_counts, location='CCTV-01', frame_number=0):
        """
        Send vehicle count data to Kafka
        
        Args:
            vehicle_counts: Dictionary with vehicle counts per class
            location: CCTV location identifier
            frame_number: Current frame number
        """
        data = {
            'timestamp': datetime.utcnow().isoformat(),
            'location': location,
            'frame_number': frame_number,
            'counts': vehicle_counts,
            'total': sum(vehicle_counts.values())
        }
        
        try:
            future = self.producer.send(self.topic, value=data)
            future.get(timeout=10)  # Wait for acknowledgment
            logging.debug(f"Data sent: {data}")
        except Exception as e:
            logging.error(f"Failed to send data to Kafka: {e}")
    
    def send_detection_data(self, detections, location='CCTV-01', frame_number=0):
        """
        Send individual detection data to Kafka
        
        Args:
            detections: List of detected vehicles with metadata
            location: CCTV location identifier
            frame_number: Current frame number
        """
        data = {
            'timestamp': datetime.utcnow().isoformat(),
            'location': location,
            'frame_number': frame_number,
            'detections': detections
        }
        
        try:
            future = self.producer.send(f"{self.topic}-detections", value=data)
            future.get(timeout=10)
            logging.debug(f"Detection data sent for {len(detections)} vehicles")
        except Exception as e:
            logging.error(f"Failed to send detection data: {e}")
    
    def close(self):
        """Close Kafka producer"""
        self.producer.flush()
        self.producer.close()
        logging.info("Kafka Producer closed")
