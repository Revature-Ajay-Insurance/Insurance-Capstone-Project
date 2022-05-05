package example

import scala.collection.mutable.ListBuffer
import java.io._
import scala.io.Source
import java.util.Properties
import scala.util.Random
import java.util.UUID
import scala.collection.JavaConverters._
import scala.util._
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


// Spark Session Imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}

object consumer{
    def main(args: Array[String]):Unit = {
        // Output Data to Kafka create new Key Value Pairs - Topic Insurance
      // Create Spark Session to Stream to Kafka
      val spark = SparkSession.builder
      .appName("KafkaSource")
      .config("spark.master", "local[*]")
      .getOrCreate()

      import spark.implicits._
      // Set Spark logging level to ERROR.
      // spark.sparkContext.setLogLevel("ERROR")
      val initDf = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
        .option("subscribe", "insurance")
        .load()
        .select(col("value").cast("string"))

      val resultDf = initDf.withColumn("value", 
        concat_ws(",",col("customer_id"),col("customer_name"),col("customer_age"),col("agent_id"),col("agent_name"),col("claim_category"),col("amount"),col("reason"),col("agent_rating"),col("datetime"),col("country"),col("state"),col("approval"), col("reimbursement_id"), col("failure_reason")))
        // .withColumn("key", col("claim_id"))

      resultDf.selectExpr("CAST(claim_id AS STRING) AS key", "CAST(value AS STRING) AS value")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
        .option("topic", "insuranceclean")
        .option("checkpointLocation", "file:///home/maria_dev/output/checkpoint/kafka_checkpoint")
        .start()
        .awaitTermination()
    }
}