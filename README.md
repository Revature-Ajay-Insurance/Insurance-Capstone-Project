# Insurance-Capstone-Project

## Table of contents
* [General info](#general-info)
* [Technologies](#technologies)
* [Setup](#setup)

## General info
Creates records on a continuous basis to set number and publishes them to a Kafka Topic to (simulate a live stream of data). The data is random with some built in trends in it. 

## Technologies
Project is created with:
* scala.collection.mutable.ListBuffer
* java.io._
* scala.io.Source
* java.util.Properties
* scala.util.Random
* java.util.UUID
* scala.collection.JavaConverters._
* scala.util._
* java.util.Properties
* org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
	
## Setup
To run this project clone it from git hub and follow these steps:\
Make sure you set how large of a csv file you want to create in the generator.scala\
Step 1:\
From prod2 folder copy the followoing files to your unix machine
```
scp -P 2222 ./names.txt maria_dev@sandbox-hdp.hortonworks.com:/home/maria_dev
scp -P 2222 ./states maria_dev@sandbox-hdp.hortonworks.com:/home/maria_dev
scp -P 2222 ./date.txt maria_dev@sandbox-hdp.hortonworks.com:/home/maria_dev
```
Step 2(create the topic in Kafka):
```
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic insurance
```
Step 3:\
to set the amount of data created change the value in the for loop in genorator.scala by passing the number you wish to create as an paramater in the producer function
```
val numTimes = 15
val numRecords = 5000
```
will generate 5,000 records 15 times

## To run generator.scala

To Run the genorator Follow these steps:\
Step 1(start sbt):
```
package 
```
Step 2(Copy the JAR file to your vm)):
```
cd target\scala-2.11
scp -P 2222 ./insurance_2.11-0.1.0-SNAPSHOT.jar maria_dev@sandbox-hdp.hortonworks.com:/home/maria_dev
```
Step 3(Run on the VM):
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 insurance-capstone-project_2.11-0.1.0-SNAPSHOT.jar
```
