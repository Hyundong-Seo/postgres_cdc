# postgres_cdc
for AGE with kafka, zookeeper and debezium connector

# Archtecture
![49259f26-e25c-4e52-9e62-9f8f6bd04de1](https://github.com/Hyundong-Seo/postgres_cdc/assets/77366371/f80a7767-86a2-4d67-905d-82c63f5bf4ca)

# Comment
PostgreSQL : Database. It is not important what you use a any version if AGE is supported a database.
Currently, AGE is supported from PG version 11 to version 14.
Debezium : This is a connector that connects PostgreSQL and Kafka.
Kafka(Producer) : Producer is responsible for receiving the change history delivered through the Debezium connector.
Kafka(Broker) : Broker is a place to store data in Kafka.
The stored data is called 'topic'.
The default storage period is 168 hours(7 days)
Kafka(Consumer) : Consumer is a role in enabling applications (java) to subscribe to topics.
You need to create a group to subscribe to a topic.
Zookeeper : ZooKeeper is used to manage and coordinate Kafka's brokers.
Java : Java is used to process data.

# Environment
Host Server : 20.04.5
Kernel : 5.4.0-153-generic
Docker version : 23.0.1
PG : 11.19
AGE : 1.3.0
Kafka : 3.0.0
Zookeeper : 3.5.9
connect : 3.0.0
