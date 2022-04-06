## Apache Kafka Course

---

Project Description
---
The project consists of 2 parts.

**The first part is about putting data to Kafka.**

In this part, students need to read a file from the local file system and put a list of accounts to the separate Kafka topic (1 message - 1 account) in CSV or JSON format.
After that using Kafka consumer API, read all data from this topic and for each account get last commits for last N intervals from the current date (N - standard interval, should be also contained in GitHub account source file, example 1d, 8h, 1w).
For getting commits from GitHub you can use GitHub Java API or rest API. 
Information about commits should be put to another topic in Kafka (1 commit - 1 message), with a username for each message (Try different key distribution).

**In the second part aggregate data from Kafka and save some metrics in the file.**

In this part, students need to read data from the commit topic, calculate metrics and write these metrics into the file. 
Exactly once semantics (Kafka Streams or KSQL can help) should be achieved. 
The format of the destination file can be CSV or JSON and can be stored in the local filesystem. 

---

Metrics to analyse
---

- Top 5 contributors by number of commits
- Total number of commits
- Total number of committers
- Total number of commits for each programming language
- Suggest four new metrics and implement them, be ready to explain their value (Optionally, but we recommend to implement at least 4 metrics )
