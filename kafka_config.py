"""
Kafka topic configuration (for Streaming Acquisition).

This file is intentionally kept as a single source of truth for:
- Topic names used by producer/consumer
- Partitioning (parallelism)
- Retention policy (how long Kafka keeps data)

Note: Kafka will NOT apply these settings automatically unless you run a topic
creation/configuration step (e.g. the provided PowerShell script).
"""

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# With docker-compose in this project, Kafka runs as a single broker => replication=1.
DEFAULT_REPLICATION_FACTOR = 1

KAFKA_TOPICS = {
    "user_events": {
        "name": "user_events",
        "partitions": 3,
        "replication": 1,
        "retention_ms": 7 * 24 * 60 * 60 * 1000,  # 7 days
        "description": "User activities and interactions",
    },
    "product_events": {
        "name": "product_events",
        "partitions": 2,
        "replication": 1,
        "retention_ms": 7 * 24 * 60 * 60 * 1000,  # 7 days
        "description": "Product views and updates",
    },
    "order_events": {
        "name": "order_events",
        "partitions": 3,
        "replication": 1,
        "retention_ms": 30 * 24 * 60 * 60 * 1000,  # 30 days
        "description": "Order processing events",
    },
    "payment_events": {
        "name": "payment_events",
        "partitions": 2,
        "replication": 1,
        "retention_ms": 30 * 24 * 60 * 60 * 1000,  # 30 days
        "description": "Payment transactions",
    },
}