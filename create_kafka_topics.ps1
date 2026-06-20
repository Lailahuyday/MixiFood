$ErrorActionPreference = "Stop"

# Creates Kafka topics and applies retention.ms based on this project's config.
# Requires: docker, docker-compose, and the `kafka` container running (from docker-compose.yml).

$topics = @(
  @{ name="user_events";    partitions=3; replication=1; retentionMs=604800000 },   # 7 days
  @{ name="product_events"; partitions=2; replication=1; retentionMs=604800000 },   # 7 days
  @{ name="order_events";   partitions=3; replication=1; retentionMs=2592000000 },  # 30 days
  @{ name="payment_events"; partitions=2; replication=1; retentionMs=2592000000 }   # 30 days
)

function ExecKafka([string]$cmd) {
  docker exec mixi-kafka bash -lc $cmd
}

function GetPartitionCount([string]$topicName) {
  $out = ExecKafka "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic $topicName"
  # Parse: "PartitionCount: 1"
  $m = [regex]::Match($out, "PartitionCount:\s*(\d+)")
  if ($m.Success) { return [int]$m.Groups[1].Value }
  return $null
}

Write-Host "Checking Kafka container..."
docker ps --format "{{.Names}}" | Select-String -Pattern "^mixi-kafka$" -Quiet | Out-Null
if ($LASTEXITCODE -ne 0) {
  throw "Container 'mixi-kafka' is not running. Start it with: docker-compose up -d"
}

foreach ($t in $topics) {
  $name = $t.name
  $partitions = $t.partitions
  $replication = $t.replication
  $retentionMs = $t.retentionMs

  Write-Host ""
  Write-Host "== Topic: $name =="

  # Create topic if it doesn't exist
  ExecKafka "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $name --partitions $partitions --replication-factor $replication"

  # If the topic already existed, it may still have default PartitionCount=1.
  # Kafka only allows INCREASING partitions (never decreasing).
  $currentPartitions = GetPartitionCount $name
  if ($null -ne $currentPartitions -and $currentPartitions -lt $partitions) {
    Write-Host "Increasing partitions: $currentPartitions -> $partitions"
    ExecKafka "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic $name --partitions $partitions"
  }

  # Apply retention policy (topic-level)
  ExecKafka "/opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name $name --alter --add-config retention.ms=$retentionMs"

  # Show final topic description
  ExecKafka "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic $name"
  ExecKafka "/opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name $name --describe | sed -n '1,6p'"
}

Write-Host ""
Write-Host "Done. Topics are created/configured."

