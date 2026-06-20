# MIXI: Hệ thống thu thập log theo thời gian thực

**MIXI** là dự án mô phỏng hệ thống thu thập dữ liệu log theo luồng (Streaming Acquisition) cho bài toán dữ liệu lớn.

Luồng chính:

**Producer (`log_generator.py`) → Kafka (4 topic) → Consumer (`consumer_test.py` hoặc `etl-service`) → HDFS / `hdfs_fallback/`**

Ngoài pipeline streaming, repo còn có Django web và một số microservice FastAPI (auth, catalog, ingestion, analytics) chạy trong Docker Compose.

---

## Tổng quan kiến trúc

| Thành phần | Vai trò |
| :--- | :--- |
| `log_generator.py` | Producer chạy trên host: mô phỏng log realtime, gửi Kafka, ghi MySQL |
| `consumer_test.py` | Consumer chạy trên host: đọc Kafka, ghi HDFS hoặc fallback local |
| `etl-service` | Consumer chạy trong Docker: đọc Kafka, ghi HDFS/fallback và ghi MySQL |
| `ingestion-service` | API FastAPI nhận event qua HTTP và gửi vào Kafka (tùy chọn) |
| `kafka` | Message broker, listener nội bộ `kafka:9092`, listener host `localhost:9093` |
| `zookeeper` | Quản lý Kafka |
| `mysql` | MySQL 8.0, host port `3307` → container `3306` |
| `namenode` / `datanode` | HDFS, WebHDFS UI tại `http://localhost:9870` |
| `web-service` | Django frontend + admin |
| `auth-service` | JWT / OAuth (Django gọi khi đăng nhập social) |
| `catalog-service` | API quản lý sản phẩm (FastAPI, độc lập) |
| `analytics-service` | API thống kê **mock** (chưa đọc MySQL/Kafka thật) |
| `gateway` | Nginx reverse proxy tới các service |

---

## Luồng dữ liệu streaming (phần cốt lõi đồ án)

### 1. Sinh log (Producer)

`log_generator.py` chạy vòng lặp liên tục:

- Mô phỏng user giả (tên, email, địa chỉ, balance, membership…)
- Lấy sản phẩm thật từ MySQL bảng `home_product`
- Random các action: `login`, `view`, `add_to_cart`, `checkout`
- Đóng gói JSON log (có `log_id`, `timestamp`, `user`, `product`, `action`, `status`, …)
- Gửi vào Kafka topic tương ứng
- Ghi bản ghi tóm tắt vào MySQL bảng `user_activity_log`

**Phân luồng topic:**

| Action | Kafka topic |
| :--- | :--- |
| `login` | `user_events` |
| `view`, `add_to_cart` | `product_events` |
| `checkout` (thành công) | `order_events` |
| `checkout` (thất bại / payment) | `payment_events` |

**Lưu ý:** `search` / `browse` không phải action riêng; chỉ xuất hiện như metadata `from_page` trong event `view`.

### 2. Kafka (Message broker)

Kafka có 2 listener:

- **Trong Docker network:** `kafka:9092` (dùng bởi `etl-service`, `ingestion-service`, …)
- **Từ máy host:** `localhost:9093` (dùng bởi `log_generator.py`, `consumer_test.py`)

4 topic được cấu hình partitions và retention trong `kafka_config.py`, áp dụng bằng script `create_kafka_topics.ps1`.

### 3. Thu thập và lưu trữ (Consumer)

Có **2 consumer** (chọn một trong hai khi demo):

#### A. `consumer_test.py` (chạy trên host)

- Subscribe 4 topic Kafka
- Với mỗi message:
  - Bổ sung `event_id` nếu thiếu
  - Ghi JSON lên HDFS qua WebHDFS
  - Nếu HDFS lỗi → ghi vào `hdfs_fallback/`
- **Không ghi MySQL** (MySQL do producer ghi)

#### B. `etl-service` (chạy trong Docker)

- Subscribe 4 topic Kafka (consumer group `etl_group`)
- Ghi HDFS hoặc fallback local
- Có logic ghi MySQL (schema phẳng `user_id`, `product_id` — phù hợp event từ `ingestion-service` hơn event từ `log_generator`)

### 4. Ghi HDFS (WebHDFS 2 bước)

1. Consumer gửi `op=CREATE` tới Namenode → nhận redirect `307`
2. Consumer PUT dữ liệu lên Datanode

`consumer_test.py` đổi hostname `datanode` → `localhost` trong URL redirect để ghi từ máy Windows.

**Đường dẫn file trên HDFS:**

```
/logs/<topic>/<YYYY-MM-DD>/<event_id>.json
```

Ví dụ:

```
/logs/user_events/2026-03-20/LOG17740174261939_213706.json
/logs/product_events/2026-03-20/LOG17740174273277_213707.json
```

> Topic ở đây là **tên đầy đủ** (`user_events`), không rút gọn thành `user`.

### 5. Fallback local

Khi HDFS không ghi được, file JSON được lưu tại:

```
hdfs_fallback/<topic>/<YYYY-MM-DD>/<timestamp>.json
```

Đây là cơ chế dự phòng để pipeline streaming **không mất dữ liệu** khi HDFS gặp sự cố.

---

## Cấu hình Kafka topic

File `kafka_config.py` khai báo:

| Topic | Partitions | Retention |
| :--- | :---: | :---: |
| `user_events` | 3 | 7 ngày |
| `product_events` | 2 | 7 ngày |
| `order_events` | 3 | 30 ngày |
| `payment_events` | 2 | 30 ngày |

Tạo và cấu hình topic (chạy sau khi Kafka đã up):

```powershell
.\create_kafka_topics.ps1
```

Script sẽ tạo topic, tăng partitions nếu cần, set `retention.ms` và in `--describe` để minh chứng.

---

## Hướng dẫn chạy

### Yêu cầu

- Docker Desktop
- Python 3.10+ (venv)
- PowerShell

### 1. Tạo file `.env` ở thư mục gốc

Docker Compose cần các biến môi trường. Tạo file `.env`:

```env
MYSQL_ROOT_PASSWORD=68686868
MYSQL_DATABASE=food
MYSQL_USER=root
MYSQL_PASSWORD=68686868
SECRET_KEY=your-secret-key-change-in-production
```

### 2. Khởi động hạ tầng

```powershell
docker compose up --build -d
```

Đợi các service healthy (đặc biệt `mixi-namenode`, `mixi-datanode`, `mixi-kafka`, `mixi-mysql`).

### 3. Tạo Kafka topic

```powershell
.\create_kafka_topics.ps1
```

### 4. Chạy consumer (terminal 1)

```powershell
.\venv\Scripts\activate
python consumer_test.py
```

### 5. Chạy producer (terminal 2)

```powershell
.\venv\Scripts\activate
python log_generator.py
```

### 6. Kiểm tra kết quả

- **Console consumer:** thấy `Wrote to HDFS: /logs/...` hoặc `Wrote fallback local file: ...`
- **HDFS UI:** `http://localhost:9870` → Browse → `/logs/user_events/2026-03-20/`
- **Fallback local:** thư mục `hdfs_fallback/` trong repo

**Xem file HDFS trên trình duyệt:** nếu link redirect sang `http://datanode:9864/...` bị lỗi DNS, thêm vào `C:\Windows\System32\drivers\etc\hosts`:

```
127.0.0.1 datanode
127.0.0.1 namenode
```

---

## Port và endpoint

| Dịch vụ | URL / Port |
| :--- | :--- |
| Django web | `http://localhost:8000` |
| Django admin | `http://localhost:8000/admin/` |
| Gateway (Nginx) | `http://localhost:80` |
| Auth service | `http://localhost:8001` |
| Catalog service | `http://localhost:8002` |
| Ingestion service | `http://localhost:8003` |
| Analytics service | `http://localhost:8004` |
| Kafka (host) | `localhost:9093` |
| MySQL (host) | `127.0.0.1:3307` |
| HDFS Namenode UI | `http://localhost:9870` |
| HDFS Datanode | `http://localhost:9864` |
| Redis | `localhost:6379` |

---

## Các thành phần phụ (ngoài pipeline streaming)

### Django web + admin

- Frontend e-commerce cơ bản (giỏ hàng, đơn hàng, …)
- Admin quản lý User, Product, Order, Payment trực tiếp trên MySQL
- **Admin không đi qua Kafka**

### Auth service

- Django `accounts/views.py` gọi `auth-service` khi đăng nhập social (Google/Facebook)
- Các phần còn lại của web chủ yếu dùng Django ORM trực tiếp

### Ingestion service (tùy chọn)

API nhận event qua HTTP và gửi vào Kafka:

```
POST http://localhost:8003/api/events/ingest
```

Schema event khác với `log_generator.py` (dùng `event_id`, `user_id` phẳng). Phù hợp khi tích hợp app thật sau này.

### Analytics service

Hiện chỉ trả **dữ liệu mock/random**, chưa kết nối MySQL hay Kafka.

### ETL service (Docker)

Chạy tự động khi `docker compose up`. Nếu demo bằng `consumer_test.py` trên host, cả hai consumer có thể cùng đọc Kafka (khác consumer group) — cân nhắc tắt `etl-service` nếu chỉ muốn demo một luồng.

---

## Cấu trúc thư mục

```
mixi/
├── docker-compose.yml          # Hạ tầng Docker
├── kafka_config.py             # Cấu hình Kafka topic
├── create_kafka_topics.ps1       # Script tạo/cấu hình topic
├── log_generator.py            # Producer mô phỏng log
├── consumer_test.py            # Consumer ghi HDFS/fallback
├── hdfs_fallback/              # Dự phòng local khi HDFS lỗi
├── mixi/                       # Django settings
├── home/, accounts/            # Django apps
├── services/
│   ├── auth-service/
│   ├── catalog-service/
│   ├── ingestion-service/
│   ├── etl-service/
│   └── analytics-service/
└── infrastructure/nginx/       # Cấu hình gateway
```

---

## Kiểm thử nhanh

- [ ] `docker ps` — tất cả container `mixi-*` đang Up
- [ ] `create_kafka_topics.ps1` — topic có đúng partitions/retention
- [ ] `log_generator.py` — console in log gửi Kafka
- [ ] `consumer_test.py` — thấy `Wrote to HDFS: /logs/...`
- [ ] HDFS UI — có file JSON trong `/logs/<topic>/<ngày>/`
- [ ] MySQL — bảng `user_activity_log` có bản ghi mới (do generator ghi)

---

## Lưu ý khi demo / báo cáo

1. **Luồng chính cần trình bày:** Producer → Kafka → Consumer → HDFS (+ fallback).
2. **MySQL trong pipeline:** do `log_generator.py` ghi khi sinh log; `consumer_test.py` không ghi MySQL.
3. **Đường dẫn HDFS** dùng tên topic đầy đủ (`user_events`, không phải `user`).
4. **Analytics service** hiện là mock — không nên mô tả là đọc dữ liệu thật từ Kafka/MySQL.
5. Nếu HDFS datanode bị lỗi clusterID, reset volume:

```powershell
docker compose down -v
docker compose up -d
```

6. `log_generator.py` kết nối MySQL tại `127.0.0.1` (port mặc định 3306). MySQL Docker map ra host port **3307** — nếu generator không connect được, chỉnh thêm `port=3307` trong file hoặc dùng MySQL local trên 3306.

---

## Đối chiếu yêu cầu đề tài Streaming Acquisition

| Yêu cầu | Trạng thái |
| :--- | :---: |
| Mô phỏng dữ liệu log theo thời gian thực | ✅ |
| Cấu hình Kafka topic (partitions, retention) | ✅ |
| Thu thập liên tục và ghi vào HDFS | ✅ |
| Cung cấp dữ liệu đầu ra cho nhóm Lưu trữ/Xử lý | ✅ |

Đầu ra chính: file JSON trên HDFS tại `/logs/<topic>/<YYYY-MM-DD>/`.  
Đầu ra dự phòng: `hdfs_fallback/<topic>/<YYYY-MM-DD>/`.
