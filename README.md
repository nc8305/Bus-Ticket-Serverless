# 🚌 Bus System Dual Architecture (Server & Serverless)

Hệ thống **Big Data & Serverless Pipeline** xử lý dữ liệu xe buýt (GPS & Đặt vé). Dự án bao gồm hai kiến trúc có thể chạy độc lập hoặc song song để so sánh và trình bày demo:

1. **Server-based (Lambda Architecture)**: Dùng Kafka, Spark, PostgreSQL cho real-time streaming và batch processing dữ liệu GPS.
2. **Serverless**: Dùng Azure API, Azure Service Bus, Azure Functions và MongoDB Atlas để xử lý đặt vé xe buýt tốc độ cao không lo sập server, không cần bảo trì hạ tầng.

![Architecture](https://img.shields.io/badge/Architecture-Dual_Mode-blue)
![Kafka](https://img.shields.io/badge/Kafka-3.x-orange)
![Spark](https://img.shields.io/badge/Spark-3.5-yellow)
![Azure](https://img.shields.io/badge/Azure-Functions-blue)
![MongoDB](https://img.shields.io/badge/MongoDB-Atlas-green)

---

## 🏗️ Hai Kiến Trúc Trọng Tâm

### 1. Server-based (Lambda Architecture)
Chuyên trị dữ liệu GPS real-time và phân tích batch cuối ngày. Chạy 100% trên container Docker nội bộ.
```text
[Load Tester] ──▶ Kafka ──▶ Spark Streaming ──▶ PostgreSQL ──▶ Grafana
```

### 2. Serverless (Azure Cloud)
Chuyên xử lý đặt vé khối lượng siêu lớn. Không tốn dung lượng ổ cứng, không cần cài đặt container. Tự động mở rộng (auto-scale).
```text
[Load Tester] ──▶ Azure API ──▶ Azure Service Bus ──▶ Azure Functions ──▶ MongoDB Atlas
```

---

## 📁 Project Structure

Cấu trúc dự án được phân chia rạch ròi thành 3 mảng độc lập, rất dễ so sánh:

```text
Bus-GPS/
├── src/
│   ├── architecture_server/        # Hệ thống Lambda cũ
│   │   ├── kafka/                  # Kafka config & tools
│   │   ├── spark/                  # Spark batch jobs
│   │   └── streaming/              # Spark Streaming / Consumer
│   │
│   └── architecture_serverless/    # Hệ thống Serverless mới
│       └── azure_worker/           # Azure Functions app (Python)
│
├── load_tester/                    # Công cụ bắn tải (Load testing)
│   └── demo_load_tester.py         # Script đa năng test cả 2 hệ thống cùng lúc
│
├── config/                         # Cấu hình Grafana/Dashboards
├── data/                           # Dữ liệu CSV mẫu
├── scripts/                        # DB Init scripts
├── docker-compose.yml              # Hạ tầng Docker cho Server-based
└── requirements.txt                # Thư viện Python
```

---

## 🚀 Hướng Dẫn Chạy Thử (Quick Start)

### 1. Cài đặt thư viện Python
Dự án sử dụng Python 3.10+. Khuyên dùng môi trường ảo (`.venv`).
```bash
pip install -r requirements.txt
pip install aiohttp aiokafka azure-functions pymongo
```

### 2. Bật Hạ Tầng Server-based (Docker)
Để chuẩn bị test nhánh Kafka/Spark/Postgres:
```bash
# Bật toàn bộ dịch vụ (Zookeeper, Kafka, Spark, DB, Grafana...)
docker-compose up -d

# Chờ khoảng 1 phút để Kafka và Postgres khởi động
# Khởi tạo Database Analytics
docker exec postgres psql -U admin -d postgres -c "CREATE DATABASE bus_analytics;"
cat scripts/init_db.sql | docker exec -i postgres psql -U admin -d bus_analytics
```

### 3. Khởi chạy Load Tester (Bắn tải dữ liệu)

Sử dụng công cụ `demo_load_tester.py` để "nã đạn" vào hệ thống.

**Chạy riêng Kafka (Server):**
```bash
python load_tester/demo_load_tester.py --mode server --requests 50000 --type gps
```
*(Lưu ý: Phải đảm bảo lệnh `docker-compose up -d` ở Bước 2 đã chạy xong).*

**Chạy riêng Azure (Serverless):**
```bash
python load_tester/demo_load_tester.py --mode serverless --requests 50000 --type ticket
```
*(Lưu ý: Phải thay thế `<LINK_AZURE_API_CUA_BAN_SAU_NAY>` trong file `demo_load_tester.py` bằng link API thật).*

**Chạy Đua SONG SONG (Trùm Cuối Demo):**
```bash
python load_tester/demo_load_tester.py --mode both --requests 50000 --type ticket
```
*(Khi chạy lệnh này, bạn sẽ thấy 2 thanh tiến trình hoặc log báo kết quả chạy đua với nhau trực tiếp trên màn hình).*

---

## 📊 Dashboard & Giám Sát

- **Grafana (Dành cho Server-based)**: http://localhost:3001 
  - Username: `admin` / Password: `admin123`
  - Dashboard nằm ở mục: `Dashboards → Bus GPS Real-time Dashboard`
- **Spark Master**: http://localhost:8082
- **pgAdmin (Quản lý Postgres)**: http://localhost:5050
  - Email: `admin@busgps.com` / Password: `admin123`

---

## 🛑 Dừng Hệ Thống Docker
Khi không cần sử dụng đến hệ thống Server-based nữa, hãy dọn dẹp để tiết kiệm RAM:
```bash
# Tắt tất cả container
docker-compose down

# Tắt và xóa luôn dữ liệu cũ (Xóa volume)
docker-compose down -v
```
