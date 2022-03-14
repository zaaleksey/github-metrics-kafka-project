1. **run zookeeper:**
```shell
zookeeper-server-start kafka-cluster/zookeeper.properties
```

2. **run kafka brokers:**
```shell
kafka-server-start kafka-cluster/broker1.properties
kafka-server-start kafka-cluster/broker2.properties
kafka-server-start kafka-cluster/broker3.properties
```

**or only one broker:**
```shell
kafka-server-start kafka-cluster/broker1.properties
```

3. **create topics:**
- github-accounts - *read from file with accounts info*
```shell
kafka-topics --bootstrap-server localhost:9092 --create --topic github-accounts \
--partitions 3 --replication-factor 3
```

- github-commits - *commits which are polled from GitHub using github-accounts topic*
```shell
kafka-topics --bootstrap-server localhost:9092 --create --topic github-commits \
--partitions 3 --replication-factor 3
```

- metrics topics
```shell
kafka-topics --bootstrap-server localhost:9092 --create \
--topic github-metrics-total-commits --partitions 2 --replication-factor 3
```
```shell
kafka-topics --bootstrap-server localhost:9092 --create \
--topic github-metrics-total-committers --partitions 2 --replication-factor 3
```
```shell
kafka-topics --bootstrap-server localhost:9092 --create \
--topic github-metrics-top-committers --partitions 2 --replication-factor 3
```
```shell
kafka-topics --bootstrap-server localhost:9092 --create \
--topic github-metrics-languages --partitions 2 --replication-factor 3
```