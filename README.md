# 🚌 Bus System Dual Architecture (Server & Serverless)

[![CI Pipeline](https://github.com/YOUR_USERNAME/YOUR_REPO/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/YOUR_REPO/actions/workflows/ci.yml)

The **Big Data & Serverless Pipeline** system processes bus data (GPS & Ticketing). The project includes two architectures that can run independently or in parallel for comparison and demo presentations:

1. **Server-based (Lambda Architecture)**: Uses Kafka, Spark, PostgreSQL for real-time streaming and batch processing of GPS data.
2. **Serverless**: Uses Azure API, Azure Service Bus, Azure Functions, and MongoDB Atlas to handle high-volume bus ticket bookings without the risk of server crashes or infrastructure maintenance.

![Architecture](https://img.shields.io/badge/Architecture-Dual_Mode-blue)
![Kafka](https://img.shields.io/badge/Kafka-3.x-orange)
![Spark](https://img.shields.io/badge/Spark-3.5-yellow)
![Azure](https://img.shields.io/badge/Azure-Functions-blue)
![MongoDB](https://img.shields.io/badge/MongoDB-Atlas-green)

---

## 🏗️ Core Architectures

### 1. Server-based (Lambda Architecture)
Specializes in real-time GPS data and end-of-day batch analysis. Runs 100% on internal Docker containers.
```text
[Load Tester] ──▶ Kafka ──▶ Spark Streaming ──▶ PostgreSQL ──▶ Grafana
```

### 2. Serverless (Azure Cloud)
Specializes in processing massive ticket booking volumes. Consumes no local disk space, requires no container setup. Automatically scales (auto-scale).
```text
[Load Tester] ──▶ Azure API ──▶ Azure Service Bus ──▶ Azure Functions ──▶ MongoDB Atlas
```

---

## 📁 Project Structure

The project structure is clearly divided into 3 independent sections, making it easy to compare:

```text
Bus-GPS/
├── src/
│   ├── architecture_server/        # Legacy Lambda system
│   │   ├── kafka/                  # Kafka config & tools
│   │   ├── spark/                  # Spark batch jobs
│   │   └── streaming/              # Spark Streaming / Consumer
│   │
│   └── architecture_serverless/    # New Serverless system
│       └── azure_worker/           # Azure Functions app (Python)
│
├── load_tester/                    # Load testing tools
│   └── demo_load_tester.py         # Multi-purpose script to test both systems simultaneously
│
├── config/                         # Grafana Configurations/Dashboards
├── data/                           # Sample CSV data
├── scripts/                        # DB Init scripts
├── docker-compose.yml              # Docker Infrastructure for Server-based
└── requirements.txt                # Python libraries
```

---

## 🚀 Quick Start Guide

### 1. Install Python libraries
The project uses Python 3.10+. It is recommended to use a virtual environment (`.venv`).
```bash
pip install -r requirements.txt
pip install aiohttp aiokafka azure-functions pymongo
```

### 2. Start Server-based Infrastructure (Docker)
To prepare and test the Kafka/Spark/Postgres branch:
```bash
# Start all services (Zookeeper, Kafka, Spark, DB, Grafana...)
docker-compose up -d

# Wait about 1 minute for Kafka and Postgres to start
# Initialize Analytics Database
docker exec postgres psql -U admin -d postgres -c "CREATE DATABASE bus_analytics;"
cat scripts/init_db.sql | docker exec -i postgres psql -U admin -d bus_analytics
```

### 3. Run Load Tester
Use the `demo_load_tester.py` tool to "bombard" the system.

**Run Kafka independently (Server):**
```bash
python load_tester/demo_load_tester.py --mode server --requests 50000 --type gps
```
*(Note: Ensure the `docker-compose up -d` command in Step 2 has finished).*

**Run Azure independently (Serverless):**
```bash
python load_tester/demo_load_tester.py --mode serverless --requests 50000 --type ticket
```
*(Note: You must replace `<LINK_AZURE_API_CUA_BAN_SAU_NAY>` in the `demo_load_tester.py` file with the real API link).*

**Run PARALLEL Race (Ultimate Demo):**
```bash
python load_tester/demo_load_tester.py --mode both --requests 50000 --type ticket
```
*(When running this command, you will see 2 progress bars or logs showing the results racing against each other directly on the screen).*

---

## 📊 Dashboards & Monitoring

- **Grafana (For Server-based)**: http://localhost:3001 
  - Username: `admin` / Password: `admin123`
  - Dashboard located at: `Dashboards → Bus GPS Real-time Dashboard`
- **Spark Master**: http://localhost:8082
- **pgAdmin (Postgres Management)**: http://localhost:5050
  - Email: `admin@busgps.com` / Password: `admin123`

---

## 🛑 Stop Docker System
When the Server-based system is no longer needed, clean it up to save RAM:
```bash
# Stop all containers
docker-compose down

# Stop and remove old data (Remove volumes)
docker-compose down -v
```
