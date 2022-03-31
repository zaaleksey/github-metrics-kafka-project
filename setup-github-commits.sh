kafka-topics --bootstrap-server localhost:9092 --create --topic github-metrics-total-commits --partitions 2 --replication-factor 2
kafka-topics --bootstrap-server localhost:9092 --create --topic github-metrics-total-committers --partitions 2 --replication-factor 2
kafka-topics --bootstrap-server localhost:9092 --create --topic github-metrics-top-committers --partitions 2 --replication-factor 2
kafka-topics --bootstrap-server localhost:9092 --create --topic github-metrics-languages --partitions 2 --replication-factor 2

curl -H 'Content-Type: application/json' \
-X POST -d '{
    "name": "filestream-sink-kafka-connector",
    "config": {
        "connector.class": "FileStreamSink",
        "tasks.max": 1,
        "topics": "github-metrics-total-commits,github-metrics-total-committers,github-metrics-top-committers,github-metrics-languages",
        "file": "/tmp/github-metrics.txt"
    }
}' \
http://localhost:8083/connectors