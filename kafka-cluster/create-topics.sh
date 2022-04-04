kafka-topics --bootstrap-server localhost:9092 --create --topic github-accounts --partitions 3 --replication-factor 3
kafka-topics --bootstrap-server localhost:9092 --create --topic github-commits --partitions 3 --replication-factor 3

kafka-topics --bootstrap-server localhost:9092 --create --topic github-metrics-total-commits --partitions 2 --replication-factor 3
kafka-topics --bootstrap-server localhost:9092 --create --topic github-metrics-total-committers --partitions 2 --replication-factor 3
kafka-topics --bootstrap-server localhost:9092 --create --topic github-metrics-top-committers --partitions 2 --replication-factor 3
kafka-topics --bootstrap-server localhost:9092 --create --topic github-metrics-languages --partitions 2 --replication-factor 3
