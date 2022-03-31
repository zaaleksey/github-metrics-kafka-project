kafka-topics --bootstrap-server localhost:9092 --create --topic github-accounts --partitions 3 --replication-factor 2
kafka-topics --bootstrap-server localhost:9092 --create --topic github-commits --partitions 3 --replication-factor 2

curl -H 'Content-Type: application/json' \
-X POST -d '{
    "name": "filestream-source-kafka-connector",
    "config": {
        "connector.class": "FileStreamSource",
        "tasks.max": 1,
        "file": "/tmp/github-accounts.txt",
        "topic": "github-accounts"
    }
}' \
http://localhost:8083/connectors