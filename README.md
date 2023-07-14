# postgres_cdc
for AGE with kafka, zookeeper and debezium connector

# Archtecture
![49259f26-e25c-4e52-9e62-9f8f6bd04de1](https://github.com/Hyundong-Seo/postgres_cdc/assets/77366371/f80a7767-86a2-4d67-905d-82c63f5bf4ca)

# Comment
PostgreSQL : Database. It is not important what you use a any version if AGE is supported a database.<br/>
Currently, AGE is supported from PG version 11 to version 14.<br/><br/>
Debezium : This is a connector that connects PostgreSQL and Kafka.<br/><br/>
Kafka(Producer) : Producer is responsible for receiving the change history delivered through the Debezium connector.<br/>
Kafka(Broker) : Broker is a place to store data in Kafka.<br/>
The stored data is called 'topic'.<br/>
The default storage period is 168 hours(7 days)<br/>
Kafka(Consumer) : Consumer is a role in enabling applications (java) to subscribe to topics.<br/>
You need to create a group to subscribe to a topic.<br/><br/>
Zookeeper : ZooKeeper is used to manage and coordinate Kafka's brokers.<br/><br/>
Java : Java is used to process data.<br/>

# Environment
Host Server : 20.04.5<br/>
Kernel : 5.4.0-153-generic<br/>
Docker version : 23.0.1<br/><br/>
PG : 11.19<br/>
AGE : 1.3.0<br/>
Kafka : 3.0.0<br/>
Zookeeper : 3.5.9<br/>
connect : 3.0.0<br/>
