import os
import json
import time
import requests
import urllib.parse
from datetime import datetime
from pathlib import Path
from kafka import KafkaConsumer
from kafka.serializer import Deserializer

KAFKA = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9093')
TOPICS = [
    'user_events',
    'product_events',
    'order_events',
    'payment_events',
]
FALLBACK_DIR = Path(os.getenv('LOCAL_FALLBACK_DIR', './hdfs_fallback'))
HDFS_NAMENODE_URL = os.getenv('HDFS_NAMENODE_URL', 'http://localhost:9870')
HDFS_USER = os.getenv('HDFS_USER', 'root')

class HDFSClient:
    def __init__(self, namenode_url: str, user: str):
        self.namenode_url = namenode_url.rstrip('/')
        self.user = user
        self.session = requests.Session()
        self.last_check = None
        self.is_available = self._test_connection()

    def _test_connection(self) -> bool:
        try:
            response = self.session.get(
                f"{self.namenode_url}/webhdfs/v1/?op=GETHOMEDIRECTORY",
                params={"user.name": self.user},
                timeout=5
            )
            self.last_check = {
                'ok': response.status_code == 200,
                'status_code': response.status_code,
                'reason': response.reason,
                'text': response.text[:400]
            }
            return response.status_code == 200
        except Exception as exc:
            self.last_check = {
                'ok': False,
                'error': str(exc)
            }
            return False

    def _ensure_directory(self, path: str) -> bool:
        try:
            params = {'op': 'MKDIRS', 'user.name': self.user}
            url = f"{self.namenode_url}/webhdfs/v1{path}"
            response = self.session.put(url, params=params, timeout=5)
            return response.status_code in [200, 201]
        except Exception:
            return False

    def _normalize_redirect_url(self, url: str) -> str:
        parsed = urllib.parse.urlparse(url)
        if parsed.hostname and parsed.hostname != 'localhost' and parsed.hostname != '127.0.0.1':
            if parsed.hostname == 'datanode' or parsed.hostname.endswith('.datanode'):
                new_netloc = f"localhost:{parsed.port}"
                parsed = parsed._replace(netloc=new_netloc)
                return urllib.parse.urlunparse(parsed)
        return url

    def write_file(self, path: str, data: dict) -> bool:
        if not self.is_available:
            return False

        try:
            payload = json.dumps(data, default=str).encode('utf-8')
            dir_path = os.path.dirname(path)
            if dir_path:
                self._ensure_directory(dir_path)

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
                    upload_url = self._normalize_redirect_url(redirect_url)
                    upload_response = self.session.put(upload_url, data=payload, timeout=10)
                    return upload_response.status_code in [200, 201]

            return False
        except Exception:
            return False

class LocalStorage:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def write_file(self, topic: str, data: dict) -> Path:
        date_str = datetime.utcnow().strftime('%Y-%m-%d')
        dir_path = self.base_dir / topic / date_str
        dir_path.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%S%f')
        filename = dir_path / f"{timestamp}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        return filename

class JSONValueDeserializer(Deserializer):
    def deserialize(self, topic: str, headers, data: bytes):
        if data is None:
            return None
        try:
            return json.loads(data.decode('utf-8'))
        except Exception:
            return None


# Ensure fallback directories exist
for t in TOPICS:
    (FALLBACK_DIR / t).mkdir(parents=True, exist_ok=True)

hdfs_client = HDFSClient(HDFS_NAMENODE_URL, HDFS_USER)
local_storage = LocalStorage(FALLBACK_DIR)

print(f"Consumer starting. Kafka={KAFKA}, topics={TOPICS}")
print(f"HDFS available: {hdfs_client.is_available} (namenode={HDFS_NAMENODE_URL})")
print(f"HDFS check: {hdfs_client.last_check}")

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA,
    value_deserializer=JSONValueDeserializer(),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='consumer_test_group'
)

try:
    for msg in consumer:
        topic = msg.topic
        value = msg.value
        if not isinstance(value, dict):
            print(f"Skipping invalid message on topic={topic}")
            continue

        event_id = value.get('event_id') or f"{topic}_{msg.partition}_{msg.offset}_{int(time.time() * 1000)}"
        value['event_id'] = event_id
        hdfs_path = f"/logs/{topic}/{datetime.utcnow().strftime('%Y-%m-%d')}/{event_id}.json"

        written_to_hdfs = False
        if hdfs_client.is_available and hdfs_client.write_file(hdfs_path, value):
            written_to_hdfs = True
            storage_message = f"Wrote to HDFS: {hdfs_path} (topic={topic} partition={msg.partition} offset={msg.offset})"
        else:
            local_file = local_storage.write_file(topic, value)
            storage_message = f"Wrote fallback local file: {local_file} (topic={topic} partition={msg.partition} offset={msg.offset})"

        action = value.get('action', '').lower()
        topic_short = topic.replace('_events', '').upper()
        user_info = value.get('user', {})
        user_id = user_info.get('id', 'N/A')
        user_name = user_info.get('name', '')
        status = value.get('status', 'unknown')
        product = value.get('product') or {}
        product_name = product.get('name', '')
        quantity = value.get('quantity')
        additional = value.get('additional_data', {})

        if action == 'login':
            event_message = f"🔐 [{topic_short}] LOGIN - User {user_id} - {user_name} - {status}"
        elif action == 'checkout':
            payment_method = additional.get('payment_method', 'N/A')
            event_message = f"{ '✅' if status == 'success' else '❌' } [{topic_short}] CHECKOUT - User {user_id} - {product_name} x{quantity} - {payment_method} - {status}"
        elif action == 'add_to_cart':
            event_message = f"👀 [{topic_short}] ADD_TO_CART - User {user_id} - {product_name} x{quantity} - {status}"
        elif action == 'view':
            event_message = f"👀 [{topic_short}] VIEW - User {user_id} - {product_name} - {status}"
        else:
            event_message = f"[{topic_short}] {action.upper() if action else 'EVENT'} - User {user_id} - {product_name} - {status}"

        print(f"{event_message} | {storage_message}")

except KeyboardInterrupt:
    print('Consumer stopped by user')
except Exception as e:
    print('Consumer error:', e)
finally:
    try:
        consumer.close()
    except:
        pass
