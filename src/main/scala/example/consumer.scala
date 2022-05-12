// spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 consumer.jar --class example.consumer
// spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 ecommerconsumer.jar
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
      spark.sparkContext.setLogLevel("ERROR")
        

      // Define Schema for Dataframe being created
      try{
      val schema = StructType(
      List(
        StructField("order_id", StringType, true),
        StructField("customer_id", StringType, true),
        StructField("customer_name", StringType, true),
        StructField("product_id", StringType, true),
        StructField("product_name", StringType, true),
        StructField("product_category", StringType, true),
        StructField("payment_type", StringType, true),
        StructField("qty", StringType, true),
        StructField("price", StringType, true),
        StructField("datetime", StringType, true),
        StructField("country", StringType, true),
        StructField("city", StringType, true),
        StructField("ecommerce_website_name", StringType, true),
        StructField("payment_txn_id", StringType, true),
        StructField("payment_txn_success", StringType, true),
        StructField("failure_reason", StringType, true)
      )
    )
      // reading from insurance topic in Kafka
      val initDf = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "insurance")
        .load()
        .select(col("value").cast("string"))
        
        // put data into schema
        val copyDF = initDf.selectExpr("CAST (value AS STRING)").toDF("value")
        copyDF.printSchema()

        val cleanDf = copyDF.select(from_json(col("value"), schema))
        cleanDf.printSchema()

        // // Output to console
        // cleanDf.writeStream
        //     .outputMode("update")
        //     .format("console")
        //     .start()
        //     .awaitTermination()
        // Output to CSV file
        cleanDf.writeStream
            .outputMode("append") // Filesink only support Append mode.
            .format("csv") // supports these formats : csv, json, orc, parquet
            .option("path", "file:///home/hdoop/output/filesink_output")
            .option("header", false)
            .option("checkpointLocation", "file:///home/hdoop/output/checkpoint/filesink_checkpoint")
            .start()
            .awaitTermination()
        
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        spark.close()
      }
    }
}