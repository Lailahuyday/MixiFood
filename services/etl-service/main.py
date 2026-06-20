import json
import os
import time
import requests
import pymysql
import logging
import threading
from kafka import KafkaConsumer
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

# ===================== LOGGING =====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ===================== CONFIGURATION =====================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "68686868")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "food")
HDFS_NAMENODE_URL = os.getenv("HDFS_NAMENODE_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "hdfs")
LOCAL_FALLBACK_DIR = os.getenv("LOCAL_FALLBACK_DIR", "/app/hdfs_fallback")

# ===================== HDFS CLIENT =====================
class HDFSClient:
    def __init__(self, namenode_url: str, user: str):
        self.namenode_url = namenode_url
        self.user = user
        self.session = requests.Session()
        self.is_available = False
        self._test_connection()

    def _test_connection(self):
        """Test HDFS connectivity"""
        try:
            response = self.session.get(
                f"{self.namenode_url}/webhdfs/v1/?op=LISTSTATUS",
                params={"user.name": self.user},
                timeout=5
            )
            self.is_available = response.status_code == 200
            logger.info(f"HDFS availability: {self.is_available}")
        except Exception as e:
            logger.warning(f"HDFS not available: {e}")
            self.is_available = False

    def write_file(self, path: str, data: Dict[str, Any]) -> bool:
        """Write file to HDFS with fallback"""
        if not self.is_available:
            return False
        
        try:
            payload = json.dumps(data, default=str).encode('utf-8')
            
            # Ensure directory exists
            dir_path = os.path.dirname(path)
            self._ensure_directory(dir_path)
            
            # Step 1: Create request
            params = {
                'op': 'CREATE',
                'user.name': self.user,
                'overwrite': 'true',
                'permission': '755'
            }
            url = f"{self.namenode_url}/webhdfs/v1{path}"
            response = self.session.put(
                url,
                params=params,
                allow_redirects=False,
                timeout=10
            )
            
            if response.status_code == 307:
                redirect_url = response.headers.get('Location')
                if redirect_url:
                    # Step 2: Upload to Datanode
                    res = self.session.put(redirect_url, data=payload, timeout=10)
                    if res.status_code in [200, 201]:
                        logger.debug(f"Successfully wrote to HDFS: {path}")
                        return True
            
            logger.warning(f"HDFS write failed with status {response.status_code}")
            return False
        except Exception as e:
            logger.error(f"HDFS Error: {e}")
            return False

    def _ensure_directory(self, path: str) -> bool:
        """Ensure directory exists in HDFS"""
        try:
            params = {'op': 'MKDIRS', 'user.name': self.user}
            url = f"{self.namenode_url}/webhdfs/v1{path}"
            response = self.session.put(url, params=params, timeout=5)
            return response.status_code in [200, 201]
        except Exception as e:
            logger.warning(f"Failed to create HDFS directory {path}: {e}")
            return False

# ===================== LOCAL FALLBACK =====================
class LocalStorage:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        Path(self.base_dir).mkdir(parents=True, exist_ok=True)

    def write_file(self, topic: str, data: Dict[str, Any]) -> bool:
        """Write file to local fallback storage"""
        try:
            date_str = datetime.now().strftime('%Y-%m-%d')
            dir_path = Path(self.base_dir) / topic / date_str
            dir_path.mkdir(parents=True, exist_ok=True)
            
            timestamp = int(time.time() * 1000)
            filename = f"LOG{timestamp}.json"
            filepath = dir_path / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, default=str)
            
            logger.debug(f"Wrote to local storage: {filepath}")
            return True
        except Exception as e:
            logger.error(f"Local storage error: {e}")
            return False

# ===================== ETL PROCESSOR =====================
class ETLProcessor:
    def __init__(self):
        self.db_conn = None
        self.hdfs_client = HDFSClient(HDFS_NAMENODE_URL, HDFS_USER)
        self.local_storage = LocalStorage(LOCAL_FALLBACK_DIR)
        
        self.stats = {
            "total_messages": 0,
            "hdfs_writes": 0,
            "local_writes": 0,
            "mysql_writes": 0,
            "failed": 0
        }
        
        self._connect_db()

    def _connect_db(self) -> bool:
        """Connect to MySQL database"""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                self.db_conn = pymysql.connect(
                    host=MYSQL_HOST,
                    user=MYSQL_USER,
                    password=MYSQL_PASSWORD,
                    database=MYSQL_DATABASE,
                    cursorclass=pymysql.cursors.DictCursor,
                    autocommit=False,
                    connect_timeout=5
                )
                logger.info("Connected to MySQL")
                return True
            except Exception as e:
                logger.warning(f"MySQL connection attempt {attempt + 1}/{max_retries} failed: {e}")
                time.sleep(2 ** attempt)
        
        logger.error("Failed to connect to MySQL after retries")
        return False

    def process_message(self, msg) -> bool:
        """Process Kafka message and save to multiple destinations"""
        try:
            topic = msg.topic
            log_data = msg.value
            
            self.stats["total_messages"] += 1
            
            # Ensure log has required fields
            if not log_data.get('event_id'):
                log_data['event_id'] = f"LOG_{int(time.time() * 1000)}"
            
            # 1. Save to HDFS (primary)
            hdfs_path = f"/logs/{topic}/{datetime.now().strftime('%Y-%m-%d')}/{log_data.get('event_id')}.json"
            if self.hdfs_client.is_available:
                if self.hdfs_client.write_file(hdfs_path, log_data):
                    self.stats["hdfs_writes"] += 1
                else:
                    # Fallback to local if HDFS fails
                    if self.local_storage.write_file(topic, log_data):
                        self.stats["local_writes"] += 1
            else:
                # HDFS unavailable, use local fallback
                if self.local_storage.write_file(topic, log_data):
                    self.stats["local_writes"] += 1
            
            # 2. Save to MySQL
            if self.db_conn:
                try:
                    with self.db_conn.cursor() as cursor:
                        sql = """INSERT INTO user_activity_log 
                                 (user_id, action, product_id, product_name, quantity, price, status, timestamp) 
                                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                        cursor.execute(sql, (
                            log_data.get('user_id'),
                            log_data.get('action'),
                            log_data.get('product_id'),
                            log_data.get('additional_data', {}).get('product_name'),
                            log_data.get('quantity'),
                            log_data.get('additional_data', {}).get('price'),
                            log_data.get('additional_data', {}).get('status', 'success'),
                            datetime.now()
                        ))
                    self.db_conn.commit()
                    self.stats["mysql_writes"] += 1
                except pymysql.Error as e:
                    logger.error(f"MySQL error: {e}")
                    self.db_conn.rollback()
                    self.stats["failed"] += 1
                    return False
            
            # Print stats every 50 messages
            if self.stats["total_messages"] % 50 == 0:
                logger.info(f"ETL Stats: {self.stats}")
            
            return True
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self.stats["failed"] += 1
            return False

    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics"""
        return self.stats.copy()

# ===================== CONSUMER LOOP =====================
def main():
    logger.info("🚀 ETL Service starting...")
    
    # Wait for infrastructure
    time.sleep(10)
    
    processor = ETLProcessor()
    
    # Start stats monitor in background
    def print_stats():
        while True:
            time.sleep(60)
            logger.info(f"ETL Statistics: {processor.get_stats()}")
    
    stats_thread = threading.Thread(target=print_stats, daemon=True)
    stats_thread.start()
    
    # Connect to Kafka
    max_kafka_retries = 5
    consumer = None
    for attempt in range(max_kafka_retries):
        try:
            consumer = KafkaConsumer(
                'user_events', 'product_events', 'order_events', 'payment_events',
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else {},
                group_id='etl_group',
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000
            )
            logger.info("Connected to Kafka Consumer")
            break
        except Exception as e:
            logger.warning(f"Kafka connection attempt {attempt + 1}/{max_kafka_retries} failed: {e}")
            time.sleep(2 ** attempt)
    
    if not consumer:
        logger.error("Failed to connect to Kafka after retries. Exiting.")
        return
    
    logger.info("📥 Listening for events...")
    
    try:
        for message in consumer:
            processor.process_message(message)
    except KeyboardInterrupt:
        logger.info("ETL Service shutting down gracefully...")
    except Exception as e:
        logger.error(f"Fatal error in consumer loop: {e}")
    finally:
        if consumer:
            consumer.close()
        if processor.db_conn:
            processor.db_conn.close()
        logger.info("ETL Service stopped")

if __name__ == "__main__":
    main()
