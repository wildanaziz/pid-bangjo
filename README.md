# Vehicle Tracking & Monitoring System

Sistem monitoring kendaraan real-time dengan integrasi Kafka, InfluxDB, dan Grafana.

## Arsitektur Sistem

```
┌─────────────────┐
│  main.py        │  ← YOLOv8 Detection + BotSort Tracking
│  (Producer)     │
└────────┬────────┘
         │ Kafka Producer
         ↓
┌─────────────────┐
│   Apache Kafka  │  ← Message Broker (Real-time Streaming)
│   Topic:        │
│ vehicle-data    │
└────────┬────────┘
         │ Kafka Consumer
         ↓
┌─────────────────┐
│ kafka_consumer  │  ← Python Consumer Script
│    .py          │
└────────┬────────┘
         │ Write Time-Series Data
         ↓
┌─────────────────┐
│   InfluxDB      │  ← Time-Series Database
│  (Storage)      │
└────────┬────────┘
         │ Query Data
         ↓
┌─────────────────┐
│    Grafana      │  ← Visualization Dashboard
│  (Dashboard)    │
└─────────────────┘
```

## Quick Start

### 1. Setup Environment

#### Buat Conda Environment:
```bash
conda create -n yolov8-tracking python=3.10 -y
conda activate yolov8-tracking
```

#### Install Dependencies:
```bash
pip install -r requirements-monitoring.txt
```

### 2. Jalankan Dengan Docker Compose (Recommended)

#### Start Infrastructure (Kafka, InfluxDB, Grafana):
```bash
docker-compose up -d
```

Tunggu beberapa detik sampai semua service ready.

#### Verifikasi Services:
```bash
docker-compose ps
```

Seharusnya menampilkan:
- `zookeeper` - Port 2181
- `kafka` - Port 9092
- `influxdb` - Port 8086
- `grafana` - Port 3000

### 3. Jalankan Sistem

#### Opsi A: Menggunakan Shell Script (Otomatis)
```bash
./start_monitoring.sh
```

Script ini akan:
1. [DONE] Check conda environment
2. [DONE] Check Kafka, InfluxDB, Grafana
3. [DONE] Start Kafka consumer (background)
4. [DONE] Start vehicle tracking system

#### Opsi B: Manual Step-by-Step

**Terminal 1 - Kafka Consumer:**
```bash
conda activate yolov8-tracking
python kafka_consumer.py
```

**Terminal 2 - Vehicle Tracking:**
```bash
conda activate yolov8-tracking
python main_integrated.py
```

### 4. Akses Dashboard

- **Grafana**: http://localhost:3000
  - Username: `admin`
  - Password: `admin`

- **InfluxDB**: http://localhost:8086
  - Username: `admin`
  - Password: `admin123456`
  - Organization: `vehicle-monitoring`
  - Bucket: `vehicle-counts`

## Konfigurasi Grafana

### Setup Data Source (InfluxDB)

1. Login ke Grafana (http://localhost:3000)
2. Pilih **Configuration** → **Data Sources**
3. Click **Add data source**
4. Pilih **InfluxDB**
5. Konfigurasi:
   ```
   Query Language: Flux
   URL: http://influxdb:8086
   Organization: vehicle-monitoring
   Token: my-super-secret-token
   Default Bucket: vehicle-counts
   ```
6. Click **Save & Test**

### Import Dashboard

1. Download dashboard JSON dari `grafana/dashboards/vehicle-dashboard.json` (akan dibuat)
2. **Dashboards** → **Import**
3. Upload JSON file atau copy-paste konten
4. Pilih InfluxDB data source
5. Click **Import**

### Query Example untuk Dashboard

**Total Vehicle Count (Time Series):**
```flux
from(bucket: "vehicle-counts")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "vehicle_count")
  |> filter(fn: (r) => r["_field"] == "total")
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
  |> yield(name: "mean")
```

**Vehicle Count by Type:**
```flux
from(bucket: "vehicle-counts")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "vehicle_count")
  |> filter(fn: (r) => r["_field"] == "car" or r["_field"] == "motorcycle" or r["_field"] == "bus" or r["_field"] == "truck")
  |> aggregateWindow(every: v.windowPeriod, fn: sum, createEmpty: false)
```

## Konfigurasi

Edit file `config.yaml` untuk menyesuaikan pengaturan:

```yaml
cctv:
  location: "CCTV-Malang-01"
  video_source: "http://stream.cctv.malangkota.go.id/..."

model:
  path: "yolov8m.pt"
  device: "cpu"  # or "cuda:0"

kafka:
  enabled: true
  servers: ["localhost:9092"]
  topic: "vehicle-data"
  send_interval: 100  # Kirim data setiap N frame

output:
  show_display: true  # Tampilkan OpenCV window
  max_frames: 5000    # 0 untuk unlimited
```

## Troubleshooting

### Kafka Connection Error
```bash
# Check Kafka is running
docker-compose ps kafka

# View Kafka logs
docker-compose logs kafka

# Restart Kafka
docker-compose restart kafka
```

### InfluxDB Connection Error
```bash
# Check InfluxDB
docker-compose ps influxdb

# Check logs
docker-compose logs influxdb

# Access InfluxDB CLI
docker exec -it influxdb influx
```

### Python Dependencies Error
```bash
# Reinstall dependencies
pip install --upgrade -r requirements-monitoring.txt
```

### OpenCV Display Error (Headless Server)
Edit `config.yaml`:
```yaml
output:
  show_display: false  # Nonaktifkan display
```

## Struktur File

```
pid-bangjo/
├── main_integrated.py          # Main script dengan Kafka
├── kafka_producer.py           # Kafka producer module
├── kafka_consumer.py           # Kafka consumer + InfluxDB writer
├── config.yaml                 # Configuration file
├── docker-compose.yml          # Infrastructure setup
├── start_monitoring.sh         # Startup script
├── requirements-monitoring.txt # Python dependencies
├── logs/                       # Log files
│   ├── tracking.log
│   └── consumer.log
└── grafana/
    ├── provisioning/
    └── dashboards/
```

## Menghentikan Sistem

### Stop Tracking Program:
- Tekan `q` di window OpenCV, atau
- Tekan `Ctrl+C` di terminal

### Stop All Services:
```bash
# If using start_monitoring.sh, just Ctrl+C

# Or manually:
docker-compose down
```

### Stop & Remove All Data:
```bash
docker-compose down -v  # Remove volumes (WARNING: Deletes all data!)
```

## Monitoring Metrics

Data yang dikirim ke Kafka/InfluxDB:

```json
{
  "timestamp": "2025-12-06T10:30:45.123Z",
  "location": "CCTV-Malang-01",
  "frame_number": 1500,
  "counts": {
    "car": 45,
    "motorcycle": 123,
    "bus": 3,
    "truck": 8
  },
  "total": 179
}
```

## Use Cases

1. **Traffic Monitoring** - Real-time vehicle counting
2. **Data Analysis** - Historical traffic patterns
3. **Alert System** - Trigger alerts on traffic congestion
4. **Multiple CCTV** - Scale to multiple locations
5. **API Integration** - Expose data via REST API

## Dependencies

- **YOLOv8** (ultralytics) - Object detection
- **BotSort** (boxmot) - Multi-object tracking
- **Apache Kafka** - Message streaming
- **InfluxDB** - Time-series database
- **Grafana** - Visualization dashboard
- **OpenCV** - Video processing

## Credits

Based on:
- YOLOv4-DeepSort by theAIGuysCode
- Ultralytics YOLOv8
- BoxMOT tracking library
