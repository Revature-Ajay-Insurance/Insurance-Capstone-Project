package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object SparkSQL {
  def queries (): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[3]")
      .appName("AjaySingala")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val sc = spark.sparkContext
    import spark.implicits._

// Treat the first row as header. Use .load() or .csv()
    println("\nRead csv file into a DF specify 1st line as header and infer the schema...")
    val df2 = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("/user/maria_dev/mergedEcommerceData.csv")   
    // df2.printSchema()
    // df2.show()

    // Running SQL Queries Programmatically.
    // Register the DataFrame as a SQL temporary view
    println("Creating View 'ecomm'...")
    df2.createOrReplaceTempView("ecomm")


    // //             TREND!!! months 10, 11, 12 sales increase by approx 50%
    println("List the sum of price for each day")
    val pnDF = spark.sql("SELECT HOUR(datetime) as Hour, sum(price) AS PriceSum FROM ecomm GROUP BY Hour ORDER BY PriceSum DESC")
    pnDF.show(numRows=pnDF.count().toInt)
    
    // //             TREND!!!
    println("Top selling products")
    val profitPCDF = spark.sql("SELECT count(payment_txn_id) AS SaleCount, sum(price), product_name FROM ecomm GROUP BY product_name ORDER BY SaleCount DESC")
    profitPCDF.show(numRows=profitPCDF.count().toInt)

    //               TREND!!! More money spent on Ecommerce every year
    println("List the average product price number in each year")
    val pnDF = spark.sql("SELECT YEAR(datetime) as Year, sum(price) AS PriceSum FROM ecomm GROUP BY Year ORDER BY PriceSum DESC")
    pnDF.show() 

    //             TREND!!! months 10, 11, 12 sales increase by approx 36%
    println("List the sum of price for each month")
    val pnDF = spark.sql("SELECT MONTH(datetime) as Month, sum(price) AS PriceSum FROM ecomm GROUP BY MONTH(datetime) ORDER BY PriceSum DESC")
    pnDF.show(numRows=pnDF.count().toInt)

    //            TREND
    // number of transactions by country in 2022
    println("List the number of transactions for each country in 2022")
    val pnDF10 = spark.sql("SELECT country, count(payment_txn_id) AS TotalOrders FROM ecomm WHERE datetime BETWEEN '2022-01-01 00:00' AND '2022-12-31 23:59' GROUP BY country")
    pnDF10.show()

    //      TREND
    // number of transactions for each year for each product
    println("List the number of distinct product_id in each year")
    val pnDF7 = spark.sql("SELECT YEAR(datetime), count(DISTINCT product_id) AS NumOfProducts FROM ecomm GROUP BY YEAR(datetime)")
    pnDF7.show() 

    // How much sales each website generates
    println("Gross sales by the 5 E-Commerce websites")
    val profitPCDF = spark.sql("SELECT sum(price) AS TotalProfit, ecommerce_website_name FROM ecomm GROUP BY ecommerce_website_name ORDER BY TotalProfit DESC")
    profitPCDF.show()

    // Failure Reasons by payment type
    println("List the Failure Reasons by Payment Type and their number of Transactions for each")
    val failurePTDF = spark.sql("SELECT count(payment_txn_id) AS TotalOrders, failure_reason AS FailureReason, payment_type FROM ecomm WHERE failure_reason IS NOT null GROUP BY failure_reason, payment_type ORDER BY TotalOrders DESC")
    failurePTDF.show(numRows=failurePTDF.count().toInt)    

    // Number of transactions for each website
    println("List the Website Names and their Number of Transactions")
    val websiteDF = spark.sql("SELECT count(payment_txn_id) AS TotalOrders, ecommerce_website_name AS WebsiteName FROM ecomm WHERE country='United States' AND ecommerce_website_name IS NOT null GROUP BY WebsiteName")
    websiteDF.show()

    // number of transactions for each product
    println("List the number of transactions for each product")
    val pnDF11 = spark.sql("SELECT count(payment_txn_id) AS TotalOrders, product_name FROM ecomm GROUP BY product_name")
    pnDF11.show(numRows=pnDF.count().toInt)

    // How much sales does each conutry have by product category
    println("How much profit does each country generate in each product category")
    val profitCOUNTRYPDF = spark.sql("SELECT sum(price) AS TotalProfit, product_category, country FROM ecomm WHERE product_category='Electronics' GROUP BY country, product_category ORDER BY country DESC, TotalProfit")
    profitCOUNTRYPDF.show(numRows=profitCOUNTRYPDF.count().toInt)

    // How much sales does each country have overall
    println("How much profit does each country generate")
    val profitCOUNTRYDF = spark.sql("SELECT sum(price) AS TotalProfit, country FROM ecomm GROUP BY country ORDER BY TotalProfit DESC")
    profitCOUNTRYDF.show()

    // The number of pet supply product each country has
    println("List Pet Supplies orders for each country")
    val pnDF = spark.sql("SELECT country, count(product_category) AS OrderCount FROM ecomm GROUP BY country ORDER BY OrderCount DESC")
    pnDF.show() 

    // The number of office products each country has
    println("List Office orders for each country")
    val pnDF2 = spark.sql("SELECT country, count(product_category) AS OrderCount FROM ecomm WHERE product_category='Office' GROUP BY country ORDER BY OrderCount DESC")
    pnDF.show() 

    // The number of home decor product each country has
    println("List Home Decor orders for each country")
    val pnDF3 = spark.sql("SELECT country, count(product_category) AS OrderCount FROM ecomm WHERE product_category='Pet Supplies' GROUP BY country ORDER BY OrderCount DESC")
    pnDF.show()

    // The number of electronic product each country has
    println("List Electronics orders for each country")
    val pnDF4 = spark.sql("SELECT country, count(product_category) AS OrderCount FROM ecomm WHERE product_category='Electronics' GROUP BY country ORDER BY OrderCount DESC")
    pnDF.show()

    // Which days have the most Insufficent Funds failure reason
    println("List days of the month that has most Insufficient Funds as failure_reason")
    val pnDF5 = spark.sql("SELECT Day(datetime) as Day, count(failure_reason) AS InsufFundsCount FROM ecomm WHERE failure_reason='Insufficient Funds' GROUP BY Day ORDER BY InsufFundsCount DESC")
    pnDF5.show() 

    // number of transactions for 2020
    println("List the number of purchases for each product_id in 2020")
    val pnDF6 = spark.sql("SELECT product_name, count(product_name) AS PurchaseCount FROM ecomm WHERE YEAR(datetime) = 2020 GROUP BY product_name ORDER BY PurchaseCount DESC")
    pnDF6.show(numRows=pnDF.count().toInt)        

    // number of transactions in US in Jan 2020
    println("List the number of transactions for US cities in January 2020")
    val pnDF8 = spark.sql("SELECT city, count(payment_txn_id) AS TotalOrders FROM ecomm WHERE country='United States' AND datetime BETWEEN '2020-01-01 00:00' AND '2020-01-31 23:59' GROUP BY city")
    pnDF8.show()    

    // Number of transactions by country in 2020
    println("List the number of transactions for each country in 2020")
    val pnDF9 = spark.sql("SELECT country, count(payment_txn_id) AS TotalOrders FROM ecomm WHERE datetime BETWEEN '2020-01-01 00:00' AND '2020-12-31 23:59' GROUP BY country")
    pnDF.show()
    
    // number of transactions by country in 2021
    println("List the number of transactions for each country in 2021")
    val pnDF9 = spark.sql("SELECT country, count(payment_txn_id) AS TotalOrders FROM ecomm WHERE datetime BETWEEN '2021-01-01 00:00' AND '2021-12-31 23:59' GROUP BY country")
    pnDF2.show()
  
   }
}