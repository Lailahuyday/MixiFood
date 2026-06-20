# MIXI — Real-time Log Collection & Data Pipeline

Hệ thống thu thập và xử lý dữ liệu log theo thời gian thực trên nền tảng **Apache Kafka** và **Hadoop HDFS**, kết hợp kiến trúc microservices cho ứng dụng thương mại điện tử.

Pipeline streaming cốt lõi:

```
log_generator.py  →  Kafka (4 topics)  →  consumer_test.py / etl-service  →  HDFS
```

---

## Tổng quan hệ thống

MIXI được xây dựng theo mô hình **event-driven architecture**: dữ liệu log phát sinh liên tục, được đẩy vào Kafka làm tầng trung gian, sau đó consumer thu thập và lưu trữ dưới dạng file JSON trên HDFS — phục vụ các tác vụ phân tích và xử lý dữ liệu lớn phía sau.

Hệ thống bao gồm:

- **Pipeline streaming**: mô phỏng log, phân phối qua Kafka, ghi HDFS
- **Hạ tầng dữ liệu**: MySQL (nghiệp vụ), Redis (cache), HDFS (lưu trữ log thô)
- **Microservices FastAPI**: xác thực, catalog, ingestion, ETL, analytics
- **Web application Django**: giao diện người dùng và trang quản trị

---

## Kiến trúc thành phần

| Thành phần | Mô tả |
| :--- | :--- |
| `log_generator.py` | Producer — mô phỏng sự kiện người dùng, gửi Kafka, ghi MySQL |
| `consumer_test.py` | Consumer — đọc Kafka, ghi HDFS hoặc fallback local |
| `etl-service` | Consumer containerized — đọc Kafka, ghi HDFS và MySQL |
| `ingestion-service` | API nhận event qua HTTP, đẩy vào Kafka |
| `auth-service` | Xác thực JWT, OAuth (Google/Facebook) |
| `catalog-service` | Quản lý sản phẩm, danh mục, tồn kho |
| `analytics-service` | API báo cáo và thống kê |
| `web-service` | Django frontend và admin panel |
| `gateway` | Nginx reverse proxy |
| `kafka` / `zookeeper` | Message broker và coordination |
| `mysql` | Cơ sở dữ liệu nghiệp vụ |
| `namenode` / `datanode` | HDFS cluster (WebHDFS) |
| `redis` | Cache và session |

---

## Luồng dữ liệu (Data Flow)

### 1. Sinh sự kiện — Producer

`log_generator.py` chạy vòng lặp liên tục, mô phỏng hành vi khách hàng trên nền tảng e-commerce:

- Tạo profile người dùng giả (họ tên, email, địa chỉ, số dư, điểm thành viên)
- Lấy danh sách sản phẩm từ MySQL (`home_product`)
- Sinh ngẫu nhiên các hành động: **login**, **view**, **add_to_cart**, **checkout**
- Đóng gói mỗi sự kiện thành document JSON
- Gửi message vào Kafka topic tương ứng
- Ghi bản ghi vào bảng `user_activity_log` trên MySQL

**Schema JSON mỗi log:**

| Trường | Mô tả |
| :--- | :--- |
| `log_id` | Định danh duy nhất |
| `timestamp` | Thời điểm sự kiện |
| `user` | Thông tin người dùng |
| `action` | Loại hành động |
| `product` | Thông tin sản phẩm (nếu có) |
| `quantity`, `status` | Số lượng và kết quả |
| `additional_data` | Chi tiết nghiệp vụ theo từng action |
| `metadata` | Nguồn log, phiên bản, phân khúc user |

**Phân luồng Kafka topic:**

| Hành động | Topic |
| :--- | :--- |
| `login` | `user_events` |
| `view`, `add_to_cart` | `product_events` |
| `checkout` thành công | `order_events` |
| `checkout` thất bại | `payment_events` |

---

### 2. Phân phối — Apache Kafka

Kafka đóng vai trò **message broker** và **buffer streaming**:

- Producer gửi log vào topic, consumer đọc độc lập theo thời gian thực
- Hệ thống hỗ trợ hai listener:
  - `kafka:9092` — giao tiếp nội bộ giữa các container
  - `localhost:9093` — kết nối từ máy host (producer/consumer chạy local)

**Cấu hình topic** (`kafka_config.py` + `create_kafka_topics.ps1`):

| Topic | Partitions | Retention |
| :--- | :---: | :---: |
| `user_events` | 3 | 7 ngày |
| `product_events` | 2 | 7 ngày |
| `order_events` | 3 | 30 ngày |
| `payment_events` | 2 | 30 ngày |

---

### 3. Thu thập — Consumer

Hệ thống cung cấp hai lớp consumer:

**`consumer_test.py`** (host)

- Subscribe 4 topic Kafka
- Xử lý từng message theo luồng streaming
- Bổ sung `event_id` khi cần
- Ghi file JSON lên HDFS qua WebHDFS
- Fallback sang `hdfs_fallback/` khi HDFS không khả dụng

**`etl-service`** (Docker)

- Consumer group `etl_group`, chạy tự động cùng stack
- Đọc Kafka, ghi HDFS hoặc fallback local
- Đồng bộ dữ liệu sang MySQL

---

### 4. Lưu trữ — HDFS

Consumer ghi log lên HDFS theo cấu trúc phân vùng thời gian:

```
/logs/<topic>/<YYYY-MM-DD>/<event_id>.json
```

Ví dụ:

```
/logs/user_events/2026-03-20/LOG17740174261939_213706.json
/logs/product_events/2026-03-20/LOG17740174273277_213707.json
/logs/order_events/2026-03-20/LOG17740174289254_180308.json
/logs/payment_events/2026-03-20/LOG17739181889254_180308.json
```

**Quy trình ghi WebHDFS:**

1. Consumer gửi request `CREATE` tới Namenode
2. Namenode trả redirect `307` sang Datanode
3. Consumer upload payload JSON lên Datanode
4. File được lưu và có thể truy cập qua HDFS Web UI

**Fallback local** — khi HDFS không ghi được, dữ liệu được lưu tại:

```
hdfs_fallback/<topic>/<YYYY-MM-DD>/<timestamp>.json
```

---

### 5. Sơ đồ luồng tổng thể

```
┌─────────────────┐     ┌──────────────────────────────────┐     ┌─────────────────┐
│ log_generator   │────▶│           Apache Kafka             │────▶│ consumer_test   │
│   (Producer)    │     │  user_events    │ product_events   │     │   (Consumer)    │
│                 │     │  order_events   │ payment_events   │     │                 │
└────────┬────────┘     └──────────────────────────────────┘     └────────┬────────┘
         │                                                                  │
         ▼                                                                  ▼
┌─────────────────┐                                              ┌─────────────────┐
│     MySQL       │                                              │      HDFS       │
│ user_activity_  │                                              │  /logs/<topic>/ │
│     log         │                                              │  hdfs_fallback/ │
└─────────────────┘                                              └─────────────────┘
```

---

## Tính năng hệ thống

### Streaming & Data Pipeline

- Thu thập log theo thời gian thực qua Apache Kafka
- Phân luồng sự kiện theo 4 topic chuyên biệt
- Cấu hình Kafka topic: partitions, replication, retention
- Ghi dữ liệu JSON lên HDFS theo cấu trúc phân vùng ngày
- Cơ chế fallback local đảm bảo liên tục khi HDFS gặp sự cố
- Xử lý WebHDFS redirect (Namenode → Datanode)

### Mô phỏng dữ liệu

- Generator sinh log liên tục với user giả và sản phẩm thật từ MySQL
- Hỗ trợ 4 loại sự kiện: login, view, add_to_cart, checkout
- Schema JSON mở rộng với metadata nghiệp vụ chi tiết
- Đồng bộ log sang MySQL (`user_activity_log`)

### Microservices

- **Auth Service** — JWT, OAuth social login
- **Catalog Service** — CRUD sản phẩm, danh mục, tồn kho
- **Ingestion Service** — API ingest event vào Kafka (`POST /api/events/ingest`)
- **ETL Service** — consumer tự động, ghi HDFS + MySQL
- **Analytics Service** — API thống kê sự kiện, timeline, doanh thu
- **Gateway (Nginx)** — reverse proxy, rate limiting

### Web Application

- Django frontend: giỏ hàng, đơn hàng, đăng nhập/đăng ký
- Django Admin: quản lý User, Product, Order, Payment
- Tích hợp Auth Service cho đăng nhập social

---

## Hướng dẫn chạy

### Yêu cầu

- Docker Desktop
- Python 3.10+
- PowerShell

### Khởi động

**1. Tạo file `.env`**

```env
MYSQL_ROOT_PASSWORD=68686868
MYSQL_DATABASE=food
MYSQL_USER=root
MYSQL_PASSWORD=68686868
SECRET_KEY=your-secret-key-change-in-production
```

**2. Khởi động toàn bộ stack**

```powershell
docker compose up --build -d
```

**3. Cấu hình Kafka topic**

```powershell
.\create_kafka_topics.ps1
```

**4. Chạy consumer (terminal 1)**

```powershell
.\venv\Scripts\activate
python consumer_test.py
```

**5. Chạy producer (terminal 2)**

```powershell
.\venv\Scripts\activate
python log_generator.py
```

**6. Xem kết quả**

- Console consumer: `Wrote to HDFS: /logs/...`
- HDFS Web UI: [http://localhost:9870](http://localhost:9870) → Browse → `/logs/`

---

## Endpoints & Ports

| Dịch vụ | URL |
| :--- | :--- |
| Django Web | `http://localhost:8000` |
| Django Admin | `http://localhost:8000/admin/` |
| Gateway (Nginx) | `http://localhost:80` |
| Auth Service | `http://localhost:8001` |
| Catalog Service | `http://localhost:8002` |
| Ingestion Service | `http://localhost:8003` |
| Analytics Service | `http://localhost:8004` |
| Kafka (host) | `localhost:9093` |
| MySQL (host) | `127.0.0.1:3307` |
| HDFS NameNode UI | `http://localhost:9870` |
| HDFS DataNode | `http://localhost:9864` |
| Redis | `localhost:6379` |

---

## Cấu trúc dự án

```
mixi/
├── docker-compose.yml          # Docker orchestration
├── kafka_config.py             # Kafka topic configuration
├── create_kafka_topics.ps1     # Topic setup script
├── log_generator.py            # Event producer
├── consumer_test.py            # Kafka consumer → HDFS
├── hdfs_fallback/              # Local fallback storage
├── mixi/                       # Django project settings
├── home/                       # Django app — products, orders
├── accounts/                   # Django app — authentication
├── services/
│   ├── auth-service/           # JWT & OAuth
│   ├── catalog-service/        # Product catalog API
│   ├── ingestion-service/      # Event ingestion API
│   ├── etl-service/            # Kafka → HDFS → MySQL
│   └── analytics-service/      # Reporting API
└── infrastructure/
    └── nginx/                  # Gateway configuration
```
