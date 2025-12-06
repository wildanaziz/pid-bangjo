"""
Vehicle Tracking & Counting System with Kafka Integration
- YOLOv8 for detection
- BotSort for tracking
- Kafka for data streaming
- Real-time visualization with OpenCV
"""

# Impor Library yang Dibutuhkan
import cv2
import numpy as np
from ultralytics import YOLO
from boxmot import BotSort
import matplotlib.pyplot as plt
import os
from pathlib import Path
import yaml
import logging

# Import Kafka Producer (optional - will work without Kafka)
try:
    from kafka_producer import VehicleDataProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    logging.warning("Kafka producer not available - running without Kafka integration")

# Setup logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/tracking.log'),
        logging.StreamHandler()
    ]
)

# Load configuration
try:
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    logging.info("Configuration loaded from config.yaml")
except FileNotFoundError:
    logging.warning("config.yaml not found, using default settings")
    config = {
        'cctv': {
            'location': 'CCTV-01',
            'video_source': 'http://stream.cctv.malangkota.go.id/WebRTCApp/streams/982131430615781858979987.m3u8?token=null'
        },
        'model': {'path': 'yolov8m.pt', 'device': 'cpu'},
        'tracker': {'reid_weights': 'osnet_x0_25_msmt17.pt', 'device': 'cpu', 'half': False},
        'output': {'video_path': 'output_pelacakan_cpu.mp4', 'show_display': True, 'max_frames': 5000},
        'kafka': {'enabled': False, 'servers': ['localhost:9092'], 'topic': 'vehicle-data', 'send_interval': 100}
    }

print("="*60)
print("VEHICLE TRACKING & COUNTING SYSTEM")
print("="*60)

# Konfigurasi Path dan Variabel
VIDEO_SOURCE = config['cctv']['video_source']
MODEL_PATH = config['model']['path']
OUTPUT_VIDEO_PATH = config['output']['video_path']
LOCATION = config['cctv']['location']
KAFKA_ENABLED = config['kafka'].get('enabled', False) and KAFKA_AVAILABLE

print(f"Lokasi        : {LOCATION}")
print(f"Model         : {MODEL_PATH}")
print(f"Video Source  : {VIDEO_SOURCE}")
print(f"Output Video  : {OUTPUT_VIDEO_PATH}")
print(f"Kafka         : {'Enabled' if KAFKA_ENABLED else 'Disabled'}")
print("="*60)

# Inisialisasi Model dan Pelacak (Tracker)
logging.info("Menginisialisasi model YOLO...")
model = YOLO(MODEL_PATH)
logging.info("Model YOLO berhasil diinisialisasi.")

logging.info("Menginisialisasi pelacak BotSort...")
tracker = BotSort(
    reid_weights=Path(config['tracker']['reid_weights']), 
    device=config['tracker']['device'], 
    half=config['tracker']['half'],   
)
logging.info("Pelacak BotSort berhasil diinisialisasi.")

# Initialize Kafka Producer
kafka_producer = None
if KAFKA_ENABLED:
    try:
        kafka_producer = VehicleDataProducer(
            kafka_servers=config['kafka']['servers'],
            topic=config['kafka']['topic']
        )
        logging.info("Kafka Producer berhasil diinisialisasi.")
    except Exception as e:
        logging.error(f"Failed to initialize Kafka: {e}")
        kafka_producer = None
        KAFKA_ENABLED = False

# Proses Video, Pelacakan, Penghitungan, dan Penyimpanan Video
logging.info("Membuka sumber video...")
cap = cv2.VideoCapture(VIDEO_SOURCE)

if not cap.isOpened():
    logging.error(f"Error: Tidak dapat membuka sumber video dari: {VIDEO_SOURCE}")
    exit(1)
else:
    # Dapatkan properti video
    w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = cap.get(cv2.CAP_PROP_FPS)
    
    if fps == 0 or fps > 100: 
        logging.warning(f"FPS dari stream tidak valid ({fps}). Menggunakan default 25 FPS.")
        fps = 25
    
    logging.info(f"Video berhasil dibuka. Resolusi: {w}x{h}, FPS: {fps}")

    # Inisialisasi VideoWriter untuk menyimpan output
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    video_writer = cv2.VideoWriter(OUTPUT_VIDEO_PATH, fourcc, fps, (w, h))
    logging.info(f"Video writer diinisialisasi untuk menyimpan ke {OUTPUT_VIDEO_PATH}.")

    # Definisikan warna untuk setiap kelas (BGR)
    color_map = {
        'car': (255, 0, 0),        # Biru
        'motorcycle': (0, 255, 0), # Hijau
        'bus': (0, 0, 255),        # Merah
        'truck': (255, 255, 0)     # Cyan
    }

    # --- Logika Penghitungan ---
    line_y = h // 2
    counting_area = {'y_start': line_y - 15, 'y_end': line_y + 15}
    vehicle_counts = {'car': 0, 'motorcycle': 0, 'bus': 0, 'truck': 0}
    tracked_ids = set()

    frame_count = 0
    max_frames = config['output'].get('max_frames', 5000)
    send_interval = config['kafka'].get('send_interval', 100)
    
    logging.info("Memulai proses tracking...")
    logging.info(f"Tekan 'q' untuk menghentikan program")
    print("\nProcessing... (Press 'q' to quit)\n")
    
    try:
        while cap.isOpened() and (max_frames == 0 or frame_count < max_frames):
            ret, frame = cap.read()
            if not ret:
                logging.info("Selesai memproses video atau stream terputus.")
                break

            # 1. Deteksi objek (hanya kelas kendaraan)
            # classes: 2=car, 3=motorcycle, 5=bus, 7=truck
            results = model.predict(
                frame, 
                device=config['model']['device'], 
                verbose=False, 
                classes=[2, 3, 5, 7]
            )

            # 2. Siapkan data deteksi untuk pelacak
            detections = []
            if results:
                for r in results:
                    for box in r.boxes:
                        x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
                        conf = box.conf[0].cpu().numpy()
                        cls = int(box.cls[0].cpu().numpy())
                        detections.append([x1, y1, x2, y2, conf, cls])

            # 3. Perbarui pelacak dengan data deteksi
            if detections:
                tracks = tracker.update(np.array(detections), frame)
            else:
                tracks = tracker.update(np.empty((0, 6)), frame)

            # 4. Logika Penghitungan & Visualisasi
            # Gambar garis hitung di separuh sisi kanan
            start_point_x = w // 2
            end_point_x = w
            cv2.line(frame, (start_point_x, line_y), (end_point_x, line_y), (0, 255, 0), 3)

            if tracks.shape[0] > 0:
                for track in tracks:
                    x1, y1, x2, y2, track_id, _, cls, _ = track
                    track_id = int(track_id)
                    cls = int(cls)
                    class_name = model.names[cls]

                    color = color_map.get(class_name, (255, 255, 255))

                    # Gambar kotak dan ID
                    cv2.rectangle(frame, (int(x1), (y1)), (int(x2), int(y2)), color, 2)
                    cv2.putText(frame, f"ID: {track_id}", (int(x1), int(y1) - 10), 
                               cv2.FONT_HERSHEY_SIMPLEX, 0.7, color, 2)

                    # Cek jika centroid objek melewati garis hitung
                    centroid_y = int((y1 + y2) / 2)
                    centroid_x = int((x1 + x2) / 2)
                    
                    # Hanya hitung jika objek berada di sisi kanan
                    if (counting_area['y_start'] < centroid_y < counting_area['y_end'] and 
                        centroid_x > start_point_x):
                        if track_id not in tracked_ids:
                            tracked_ids.add(track_id)
                            if class_name in vehicle_counts:
                                vehicle_counts[class_name] += 1
                            # Tandai objek yang dihitung dengan lingkaran kuning
                            cv2.circle(frame, (centroid_x, centroid_y), 10, (0, 255, 255), -1)
            
            # Tampilkan total hitungan di layar
            y_offset = 40
            cv2.putText(frame, f"{LOCATION} - Jumlah Kendaraan:", (15, y_offset), 
                       cv2.FONT_HERSHEY_SIMPLEX, 0.8, (255, 255, 255), 2, cv2.LINE_AA)
            y_offset += 40

            display_names = {'car': 'Mobil', 'motorcycle': 'Motor', 'bus': 'Bus', 'truck': 'Truk'}
            
            for class_key, count in vehicle_counts.items():
                display_name = display_names.get(class_key, class_key)
                text = f"{display_name}: {count}"
                color = color_map.get(class_key, (255, 255, 255))
                cv2.putText(frame, text, (15, y_offset), 
                           cv2.FONT_HERSHEY_SIMPLEX, 0.8, color, 2, cv2.LINE_AA)
                y_offset += 35

            # Tulis frame ke file video
            video_writer.write(frame)

            # Tampilkan canvas OpenCV
            if config['output'].get('show_display', True):
                cv2.imshow(f'Vehicle Tracking - {LOCATION}', frame)
                
                # Tekan 'q' untuk keluar
                if cv2.waitKey(1) & 0xFF == ord('q'):
                    logging.info("User menghentikan program (tekan 'q')")
                    break
            
            # Send data to Kafka periodically
            if kafka_producer and frame_count % send_interval == 0:
                try:
                    kafka_producer.send_count_data(
                        vehicle_counts=vehicle_counts,
                        location=LOCATION,
                        frame_number=frame_count
                    )
                except Exception as e:
                    logging.error(f"Failed to send to Kafka: {e}")

            # Progress logging
            if frame_count % 100 == 0:
                total = sum(vehicle_counts.values())
                logging.info(f"Frame {frame_count} | Total: {total} | "
                           f"Car: {vehicle_counts['car']} | Motor: {vehicle_counts['motorcycle']} | "
                           f"Bus: {vehicle_counts['bus']} | Truck: {vehicle_counts['truck']}")

            frame_count += 1

    except KeyboardInterrupt:
        logging.info("Program dihentikan oleh user (Ctrl+C)")
    
    finally:
        # Setelah loop selesai
        print("\n" + "="*60)
        print("PROSES SELESAI")
        print("="*60)
        print(f"Lokasi: {LOCATION}")
        print(f"Total Frame: {frame_count}")
        print("\nHasil Akhir Hitungan:")
        for cls, count in vehicle_counts.items():
            print(f"  {display_names.get(cls, cls):12s}: {count}")
        print(f"  {'Total':12s}: {sum(vehicle_counts.values())}")
        print("="*60)

        # Send final data to Kafka
        if kafka_producer:
            try:
                kafka_producer.send_count_data(
                    vehicle_counts=vehicle_counts,
                    location=LOCATION,
                    frame_number=frame_count
                )
                kafka_producer.close()
                logging.info("Kafka producer closed")
            except Exception as e:
                logging.error(f"Error closing Kafka: {e}")

        # Cleanup
        cap.release()
        video_writer.release()
        cv2.destroyAllWindows()
        
        logging.info(f"Video output disimpan di: {OUTPUT_VIDEO_PATH}")
        logging.info(f"Total frames processed: {frame_count}")
        logging.info(f"Final counts: {vehicle_counts}")
        
        print(f"\nVideo tersimpan di: {OUTPUT_VIDEO_PATH}")
        print(f"Log file: logs/tracking.log\n")
