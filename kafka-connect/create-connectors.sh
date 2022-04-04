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