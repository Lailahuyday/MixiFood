# 🚀 MIXI: E-commerce Microservices Data Pipeline

**MIXI** là hệ thống thương mại điện tử chạy trên kiến trúc **microservices** với luồng dữ liệu thời gian thực.
Hệ thống tích hợp Kafka làm message broker, MySQL làm cơ sở dữ liệu nghiệp vụ và Hadoop HDFS làm lớp lưu trữ log thô.

---

## 🌟 Tổng quan

MIXI gồm:
- Django `web-service` làm frontend chính, hiển thị giỏ hàng, đơn hàng và các tính năng cơ bản của e-commerce.
- Các microservice FastAPI để xử lý xác thực, catalog sản phẩm, ingest event, ETL và analytics.
- Kafka dùng để truyền tải sự kiện giữa dịch vụ và pipeline xử lý.
- MySQL lưu trữ dữ liệu nghiệp vụ chính như user, sản phẩm, đơn hàng và lịch sử hoạt động.
- Hadoop HDFS lưu trữ log thô của sự kiện để phục vụ phân tích và xử lý lịch sử.
- Nếu HDFS không ghi được, pipeline sẽ dùng thư mục `hdfs_fallback/` làm dự phòng local.

---

## 🧩 Các thành phần chính

| Thành phần | Chức năng |
| :--- | :--- |
| `web-service` | Django frontend hiển thị UI, gọi API đến các dịch vụ backend |
| `auth-service` | Xác thực, đăng ký, đăng nhập, JWT |
| `catalog-service` | Quản lý sản phẩm, category, tồn kho |
| `ingestion-service` | Nhận event từ web/app và gửi vào Kafka |
| `etl-service` | Tiêu thụ Kafka, ghi MySQL, ghi HDFS hoặc fallback local |
| `analytics-service` | API báo cáo, truy vấn chỉ số kinh doanh |
| `kafka` | Broker message Kafka với listener host `localhost:9093` và nội bộ `kafka:9092` |
| `zookeeper` | Quản lý Kafka |
| `mysql` | MySQL 8.0 cho dữ liệu nghiệp vụ |
| `redis` | Bộ nhớ đệm và session |
| `namenode` | HDFS NameNode, cung cấp WebHDFS |
| `datanode` | HDFS DataNode, lưu trữ block và nhận upload từ redirect WebHDFS |

---

## ✅ Tính năng hiện có

- Hệ thống Django + FastAPI chạy trong Docker Compose.
- Kafka phân luồng sự kiện theo nhiều topic: `user_events`, `product_events`, `order_events`, `payment_events`.
- `log_generator.py` tạo event giả, gửi Kafka và ghi log vào MySQL.
- `consumer_test.py` đọc Kafka, ghi file log vào HDFS, fallback về `hdfs_fallback/` khi HDFS không khả dụng.
- Django admin có thể quản lý User, Product, Order, OrderItem và Payment.
- URL admin Django: `/admin/`.
- HDFS WebHDFS được cấu hình để cho phép tạo file từ host Docker.
- Port mappings cho phép truy cập từ host: Kafka `9093`, MySQL `3307`, Redis `6379`, HDFS NameNode `9870`, DataNode `9864`, và frontend/service ports.

---

## 🔄 Luồng hoạt động người dùng và admin

### 1. User events

Người dùng trên frontend có thể tạo ra các event chính sau:
- `login`: đăng nhập / đăng ký
- `view`: xem chi tiết sản phẩm
- `add_to_cart`: thêm sản phẩm vào giỏ hàng
- `checkout`: thanh toán đơn hàng
- `search` / `browse`: duyệt danh mục, tìm kiếm sản phẩm (với dữ liệu đầu vào được mô phỏng bởi generator)

Các event này được gửi vào Kafka và lưu đồng thời vào MySQL để phục vụ giao diện và lịch sử.

### 2. Admin events

Admin hoạt động qua Django admin tại `http://localhost:8000/admin/` và có thể tạo ra / sửa / xóa các loại nghiệp vụ:
- `Manage User`: tạo, sửa thông tin, xem balance, membership, trạng thái tài khoản.
- `Manage Product`: thêm mới sản phẩm, cập nhật giá, tồn kho, thông tin category.
- `Manage Order`: kiểm tra đơn hàng, trạng thái đơn, tổng tiền, chi tiết item.
- `Manage Payment`: xem trạng thái thanh toán, phương thức và lịch sử giao dịch.

Những thao tác này lưu trực tiếp vào MySQL. Admin dùng Django admin để giám sát dữ liệu nghiệp vụ, không phải luồng Kafka.

### 3. Sinh event ngẫu nhiên mô phỏng khách hàng

`log_generator.py` là công cụ tạo event giả cho hệ sinh thái big data:
- tạo người dùng giả với tên, email, địa chỉ, balance, membership.
- lấy sản phẩm real từ bảng `home_product` trong MySQL.
- sinh ngẫu nhiên các event `login`, `view`, `add_to_cart`, `checkout`.
- gửi event vào Kafka topic tương ứng:
  - `user_events`
  - `product_events`
  - `order_events`
  - `payment_events`
- ghi cùng event vào MySQL table `user_activity_log`.

### 4. Mục đích mô phỏng

Mục tiêu của generator là tạo ra dữ liệu khách hàng giả để:
- thử luồng dữ liệu thời gian thực trên Kafka
- kiểm tra khả năng nhập liệu của ETL pipeline
- tạo dữ liệu log thô để lưu trữ trong HDFS
- mô phỏng một hệ sinh thái big data với event streaming và storage

## 🔄 Luồng dữ liệu chi tiết (Workflow)

### 1. Sinh sự kiện

- `log_generator.py` tạo event giả dựa trên sản phẩm và người dùng.
- Events có thể là `login`, `view`, `add_to_cart`, `checkout`.
- Mỗi event được gửi tới Kafka topic tương ứng và đồng thời ghi vào MySQL.

### 2. Đẩy event vào Kafka

- `ingestion-service` (hoặc `log_generator.py`) gửi event vào Kafka.
- Kafka có 2 listener:
  - Nội bộ giữa container: `kafka:9092`
  - Host máy local: `localhost:9093`
- `log_generator.py` chạy từ host nên dùng `localhost:9093`.

### 3. Tiêu thụ event trong ETL

- `consumer_test.py` hoặc `etl-service` đọc từ các topic Kafka:
  - `user_events`
  - `product_events`
  - `order_events`
  - `payment_events`
- Với mỗi message:
  - Chuẩn hóa thành JSON
  - Thêm `event_id`
  - Tạo HDFS path dạng `/logs/<topic>/<YYYY-MM-DD>/<event_id>.json`

### 4. Ghi vào HDFS hoặc fallback

- Nếu HDFS NameNode và DataNode khả dụng, consumer dùng WebHDFS `CREATE`.
- HDFS trả về redirect `307` tới DataNode.
- `consumer_test.py` xử lý redirect và đổi hostname DataNode sang `localhost:9864` khi cần.
- Nếu ghi HDFS thành công:
  - File được lưu vào HDFS path `/logs/...`
  - Log hiển thị: `Wrote to HDFS: /logs/...`
- Nếu HDFS không ghi được:
  - File được lưu local vào `hdfs_fallback/<topic>/<YYYY-MM-DD>/<timestamp>.json`
  - Log hiển thị: `Wrote fallback local file: ...`

### 5. Lưu trữ nghiệp vụ

- Cùng lúc event được viết MySQL bằng `insert_activity_log()` trong `log_generator.py`.
- MySQL chứa bảng `user_activity_log` và các bảng nghiệp vụ của Django.
- Dữ liệu chính được dùng cho giao diện, báo cáo và truy vấn.

---

## 🧠 Quy trình hoạt động chi tiết

### A. Khởi động hệ thống

1. Chạy `docker compose up --build -d`
2. Docker tạo và kết nối dịch vụ vào mạng `mixi-network`
3. Kafka, Zookeeper, MySQL, Redis, HDFS, gateway, API services và web-service đều up

### B. Giao tiếp giữa dịch vụ

- `web-service` gọi API nội bộ tới `auth-service`, `catalog-service`, `ingestion-service`.
- `ingestion-service` viết event vào Kafka nội bộ `kafka:9092`.
- `etl-service` đọc Kafka nội bộ và ghi HDFS nội bộ qua `namenode:9870`.
- `analytics-service` đọc từ MySQL và Kafka để trả báo cáo.

### C. HDFS Fallback

- `etl-service` và `consumer_test.py` được cấu hình `HDFS_USER=root`.
- Khi HDFS không sẵn sàng, thư mục `hdfs_fallback/` được tạo tự động.
- `hdfs_fallback/` là nơi chứa bản sao log JSON local để không mất dữ liệu khi HDFS lỗi.
- Đây không phải là HDFS thực sự mà chỉ là cơ chế dự phòng.

### D. Kiểm tra trạng thái

- Kiểm tra HDFS NameNode UI: `http://localhost:9870`
- Kiểm tra DataNode status: `http://localhost:9864` (qua host mapping)
- Kiểm tra Kafka broker bằng `localhost:9093`
- Kiểm tra MySQL bằng `localhost:3307`
- Kiểm tra các service FastAPI:
  - `http://localhost:8001` (auth)
  - `http://localhost:8002` (catalog)
  - `http://localhost:8003` (ingestion)
  - `http://localhost:8004` (analytics)
- Kiểm tra Django frontend: `http://localhost:8000`

---

## 📌 Hướng dẫn chạy nhanh

### 1. Khởi động toàn bộ stack

```powershell
docker compose up --build -d
```

### 2. Chạy generator event từ host

```powershell
python log_generator.py
```

### 3. Chạy consumer kiểm tra HDFS / fallback

```powershell
python consumer_test.py
```

---

## 🧪 Mục tiêu kiểm thử

- Kiểm tra Kafka đang nhận event trên `localhost:9093`.
- Kiểm tra MySQL host `127.0.0.1:3307` với database `food`.
- Kiểm tra `consumer_test.py` có thể kết nối HDFS NameNode và DataNode.
- Kiểm tra `hdfs_fallback/` có file khi HDFS không ghi được.
- Kiểm tra UI Django và các API FastAPI hoạt động.

---

## 📁 Lưu ý cấu trúc repo

- `hdfs_fallback/`: dự phòng local nếu HDFS fails.
- `docker-compose.yml`: cấu trúc Docker toàn hệ thống.
- `log_generator.py`: tạo event giả, gửi Kafka, ghi MySQL.
- `consumer_test.py`: consumer Kafka + ghi HDFS / fallback.
- `mixi/`: cấu hình Django.
- `home/`, `accounts/`: app Django.
- `services/`: chứa các dịch vụ FastAPI.

---

