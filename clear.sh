echo -n "" > /tmp/github-accounts.txt
rm -rf /tmp/kafka-cluster

kafka-topics --bootstrap-server localhost:9092 --delete --topic github-accounts
kafka-topics --bootstrap-server localhost:9092 --delete --topic github-commits

kafka-topics --bootstrap-server localhost:9092 --delete --topic github-metrics-total-commits
kafka-topics --bootstrap-server localhost:9092 --delete --topic github-metrics-total-committers
kafka-topics --bootstrap-server localhost:9092 --delete --topic github-metrics-top-committers
kafka-topics --bootstrap-server localhost:9092 --delete --topic github-metrics-languages
