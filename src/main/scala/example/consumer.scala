
// spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 consumer.jar --class example.consumer

package example

import java.util.{Collections, Properties}
import java.util.regex.Pattern
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._


// Spark Session Imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
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
        

      // Define Schema for Dataframe being created
      val schema = StructType(
      List(
        StructField("claim_id", StringType, true),
        StructField("customer_id", StringType, true),
        StructField("customer_name", StringType, true),
        StructField("Customer_age", StringType, true),
        StructField("agent_id", StringType, true),
        StructField("agent_name", StringType, true),
        StructField("claim_category", StringType, true),
        StructField("amount", StringType, true),
        StructField("reason", StringType, true),
        StructField("agent_rating", StringType, true),
        StructField("datetime", StringType, true),
        StructField("country", StringType, true),
        StructField("state", StringType, true),
        StructField("approval", StringType, true),
        StructField("reimbursement_id", StringType, true),
        StructField("failure_reason", StringType, true)
      )
    )
      // reading from insurance topic in Kafka
      val initDf = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
        .option("subscribe", "insurance")
        .load()
        .select(col("value").cast("string"))
        
        // put data into schema
        val copyDF = initDf.selectExpr("CAST (value AS STRING)").toDF("value")
        copyDF.printSchema()

        val cleanDf = copyDF.select(from_json(col("value"), schema))
        cleanDf.printSchema()

        // Output to console
        cleanDf.writeStream
            .outputMode("update")
            .format("console")
            .start()
            .awaitTermination()
        // val view = cleanDf.writeStream
        //     .outputMode("complete")
        //     .option("truncate", false)
        //     .format("console")
        //     .start()
        // view.awaitTermination()
        // Data Transformations/aggregations
    //   val resultDf = initDf.withColumn("value", 
    //     concat_ws(",",col("claim_id"),col("customer_id"),col("customer_name"),col("customer_age"),col("agent_id"),col("agent_name"),col("claim_category"),col("amount"),col("reason"),col("agent_rating"),col("datetime"),col("country"),col("state"),col("approval"), col("reimbursement_id"), col("failure_reason")))
        // .withColumn("key", col("claim_id"))
        
        // Write Data transformations into a new topic

    // def main(args: Array[String]):Unit = {
    //     // Output Data to Kafka create new Key Value Pairs - Topic Insurance
    //   // Create Spark Session to Stream to Kafka
    //   val spark = SparkSession.builder
    //   .appName("KafkaSource")
    //   .config("spark.master", "local[*]")
    //   .getOrCreate()

    //   import spark.implicits._
    //   // Set Spark logging level to ERROR.
    //   // spark.sparkContext.setLogLevel("ERROR")
    //   val initDf = spark.readStream
    //     .format("kafka")
    //     .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
    //     .option("subscribe", "insurance")
    //     .load()
    //     .select(col("value").cast("string"))

    //     // Data Transformations/aggregations
    //   val resultDf = initDf.withColumn("value", 
    //     concat_ws(",",col("customer_id"),col("customer_name"),col("customer_age"),col("agent_id"),col("agent_name"),col("claim_category"),col("amount"),col("reason"),col("agent_rating"),col("datetime"),col("country"),col("state"),col("approval"), col("reimbursement_id"), col("failure_reason")))
    //     // .withColumn("key", col("claim_id"))

    //     // Write Data transformations into a new topic

    //   resultDf.selectExpr("CAST(claim_id AS STRING) AS key", "CAST(value AS STRING) AS value")
    //     .writeStream
    //     .format("kafka")
    //     .option("kafka.bootstrap.servers", "localhost:9092")
    //     .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
    //     .option("topic", "insuranceclean")
    //     .option("checkpointLocation", "file:///home/maria_dev/output/checkpoint/kafka_checkpoint")
    //     .start()
    //     .awaitTermination()

    }

    // def main(args: Array[String]):Unit = {
    //     val props: Properties = new Properties()
    //     props.put("group.id", "test-consumer-group") //need group id from
    //     props.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
    //     props.put(
    //     "key.deserializer",
    //     "org.apache.kafka.common.serialization.StringDeserializer"
    //     )
    //     props.put(
    //     "value.deserializer",
    //     "org.apache.kafka.common.serialization.StringDeserializer"
    //     )
    //     props.put("enable.auto.commit", "true")
    //     props.put("auto.commit.interval.ms", "2000")
    //     val consumer = new KafkaConsumer(props)
    //     val topics = List("insurance")
    //     try {
    //     consumer.subscribe(topics.asJava) //subscribe to topic as a type Java
    //     while (true) {
    //         val records = consumer.poll(10) //read one record every 10 seconds
    //         for (record <- records.asScala) { // convert the read records into Scala code
    //         println( //for each record print Topic, Key, Value, Offset, Partiton
    //             // "Topic: " + record.topic() +
    //             ",Key: " + record.key() +
    //             ",Value: " + record.value() +
    //             // ", Offset: " + record.offset() +
    //             ", Partition: " + record.partition()
    //         )
    //         }
    //     }
    //     } catch {
    //     case e: Exception => e.printStackTrace()
    //     } finally {
    //     consumer.close()
    //     }
    // }
}
    // }

