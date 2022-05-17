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

    // println("How much profit is generated Monday - Sunday")
    // val msDF = spark.sql("SELECT WEEKDAY(datetime) AS WeekdayNumber, sum(price) as TotalProfit FROM ecomm GROUP BY WeekdayNumber ORDER BY TotalProfit")
    // msDF.show()

    // // //             TREND!!! months 10, 11, 12 sales increase by approx 50%
    // println("List the sum of price for each day")
    // val pnDF = spark.sql("SELECT HOUR(datetime) as Hour, sum(price) AS PriceSum FROM ecomm GROUP BY Hour ORDER BY PriceSum DESC")
    // pnDF.show(numRows=pnDF.count().toInt)
    
    // // //             TREND!!!
    // println("Top selling products")
    // val profitPCDF = spark.sql("SELECT count(payment_txn_id) AS SaleCount, sum(price), product_name FROM ecomm GROUP BY product_name ORDER BY SaleCount DESC")
    // profitPCDF.show(numRows=profitPCDF.count().toInt)

    // println("Gross sales by the 5 E-Commerce websites")
    // val profitPCDF = spark.sql("SELECT sum(price) AS TotalProfit, ecommerce_website_name FROM ecomm GROUP BY ecommerce_website_name ORDER BY TotalProfit DESC")
    // profitPCDF.show()

    // println("List the Failure Reasons by Payment Type and their number of Transactions for each")
    // val failurePTDF = spark.sql("SELECT count(payment_txn_id) AS TotalOrders, failure_reason AS FailureReason, payment_type FROM ecomm WHERE failure_reason IS NOT null GROUP BY failure_reason, payment_type ORDER BY TotalOrders DESC")
    // failurePTDF.show(numRows=failurePTDF.count().toInt)    

    // println("List the Website Names and their Number of Transactions")
    // val websiteDF = spark.sql("SELECT count(payment_txn_id) AS TotalOrders, ecommerce_website_name AS WebsiteName FROM ecomm WHERE country='United States' AND ecommerce_website_name IS NOT null GROUP BY WebsiteName")
    // websiteDF.show()

    // println("How much profit does each country generate in each product category")
    // val profitCOUNTRYPDF = spark.sql("SELECT sum(price) AS TotalProfit, product_category, country FROM ecomm WHERE product_category='Electronics' GROUP BY country, product_category ORDER BY country DESC, TotalProfit")
    // profitCOUNTRYPDF.show(numRows=profitCOUNTRYPDF.count().toInt)
    // println("How much profit does each country generate")
    // val profitCOUNTRYDF = spark.sql("SELECT sum(price) AS TotalProfit, country FROM ecomm GROUP BY country ORDER BY TotalProfit DESC")
    // profitCOUNTRYDF.show()

    // println("List Pet Supplies orders for each country")
    // val pnDF = spark.sql("SELECT country, count(product_category) AS OrderCount FROM ecomm GROUP BY country ORDER BY OrderCount DESC")
    // pnDF.show() 
    // println("List Office orders for each country")
    // val pnDF2 = spark.sql("SELECT country, count(product_category) AS OrderCount FROM ecomm WHERE product_category='Office' GROUP BY country ORDER BY OrderCount DESC")
    // pnDF.show() 
    // println("List Home Decor orders for each country")
    // val pnDF3 = spark.sql("SELECT country, count(product_category) AS OrderCount FROM ecomm WHERE product_category='Pet Supplies' GROUP BY country ORDER BY OrderCount DESC")
    // pnDF.show()
    // println("List Electronics orders for each country")
    // val pnDF4 = spark.sql("SELECT country, count(product_category) AS OrderCount FROM ecomm WHERE product_category='Electronics' GROUP BY country ORDER BY OrderCount DESC")
    // pnDF.show()

    // //  No trend :(
    // println("List days of the month that has most Insufficient Funds as failure_reason")
    // val pnDF = spark.sql("SELECT Day(datetime) as Day, count(failure_reason) AS InsufFundsCount FROM ecomm WHERE failure_reason='Insufficient Funds' GROUP BY Day ORDER BY InsufFundsCount DESC")
    // pnDF.show()    

    // println("List the average product price number in each year")
    // val pnDF = spark.sql("SELECT country, count(payment_txn_id) AS SuccPayNum FROM ecomm WHERE payment_txn_seccess='N' GROUP BY country ORDER BY country DESC")
    // pnDF.show() 

    // //               TREND!!! More money spent on Ecommerce every year
    // println("List the average product price number in each year")
    // val pnDF = spark.sql("SELECT YEAR(datetime) as Year, sum(price) AS PriceSum FROM ecomm GROUP BY Year ORDER BY PriceSum DESC")
    // pnDF.show()      

    // //             TREND!!! months 10, 11, 12 sales increase by approx 36%
    // println("List the sum of price for each month")
    // val pnDF = spark.sql("SELECT MONTH(datetime) as Month, sum(price) AS PriceSum FROM ecomm GROUP BY MONTH(datetime) ORDER BY PriceSum DESC")
    // pnDF.show(numRows=pnDF.count().toInt)         

    // println("List the number of purchases for each product_id in 2020")
    // val pnDF = spark.sql("SELECT product_name, count(product_name) AS PurchaseCount FROM ecomm WHERE YEAR(datetime) = 2020 GROUP BY product_name ORDER BY PurchaseCount DESC")
    // pnDF.show(numRows=pnDF.count().toInt)     

    // println("List the number of distinct product_id in each year")
    // val pnDF = spark.sql("SELECT YEAR(datetime), count(DISTINCT product_id) AS NumOfProducts FROM ecomm GROUP BY YEAR(datetime)")
    // pnDF.show()    

    // println("List the number of transactions for US cities in January 2020")
    // val pnDF = spark.sql("SELECT city, count(payment_txn_id) AS TotalOrders FROM ecomm WHERE country='United States' AND datetime BETWEEN '2020-01-01 00:00' AND '2020-01-31 23:59' GROUP BY city")
    // pnDF.show()    

    // println("List the number of transactions for each country in 2020")
    // val pnDF = spark.sql("SELECT country, count(payment_txn_id) AS TotalOrders FROM ecomm WHERE datetime BETWEEN '2020-01-01 00:00' AND '2020-12-31 23:59' GROUP BY country")
    // pnDF.show()
    // println("List the number of transactions for each country in 2021")
    // val pnDF2 = spark.sql("SELECT country, count(payment_txn_id) AS TotalOrders FROM ecomm WHERE datetime BETWEEN '2021-01-01 00:00' AND '2021-12-31 23:59' GROUP BY country")
    // pnDF2.show()
    // println("List the number of transactions for each country in 2022")
    // val pnDF3 = spark.sql("SELECT country, count(payment_txn_id) AS TotalOrders FROM ecomm WHERE datetime BETWEEN '2022-01-01 00:00' AND '2022-12-31 23:59' GROUP BY country")
    // pnDF3.show()


    // println("List the number of transactions for each product")
    // val pnDF = spark.sql("SELECT count(payment_txn_id) AS TotalOrders, product_name FROM ecomm GROUP BY product_name")
    // pnDF.show(numRows=pnDF.count().toInt)

    // println("List the number of transactions for each product category")
    // val pcDF = spark.sql("SELECT count(payment_txn_id) AS TotalOrders, product_category FROM ecomm GROUP BY product_category")
    // pcDF.show()

    // println("List the number of transactions for each Country, City pair")
    // // val dateDF = spark.sql("SELECT * FROM ecomm WHERE datetime BETWEEN '19:05' AND '23:55'")
    // val dateDF = spark.sql("SELECT * FROM ecomm WHERE datetime BETWEEN '2019-10-17 20:05' AND '2019-10-18 20:05'")
    // dateDF.show(numRows=dateDF.count().toInt)

    // //Added order_id to help with data cleaning
    // println("How much profit did each product generate")
    // val profitPNDF = spark.sql("SELECT order_id, sum(price) AS TotalProfit, country FROM ecomm GROUP BY country, order_id ORDER BY TotalProfit DESC")
    // profitPNDF.show(numRows=profitPNDF.count().toInt)

    // println("Display states with highest number of approved claims (57 approvals or higher).....")
    // val sqlDF = spark.sql("SELECT sum(price) AS TotalProfit, product_category FROM ecomm GROUP BY product_category ORDER BY TotalProfit DESC")
    // val sqlDF = spark.sql("SELECT sum(price) AS TotalProfit, country FROM ecomm GROUP BY country ORDER BY TotalProfit DESC")
    // val sqlDF = spark.sql("SELECT sum(price) AS TotalProfit, country, product_category FROM ecomm GROUP BY country, product_category ORDER BY country DESC, TotalProfit")
    // val sqlDF = spark.sql("SELECT count(payment_txn_id) AS TotalOrders, ecommerce_website_name AS WebsiteName FROM ecomm WHERE ecommerce_website_name IS NOT null GROUP BY ecommerce_website_name ORDER BY TotalOrders DESC")

    // println("List the Website Names and their Number of Transactions")
    // val websiteDF = spark.sql("SELECT count(payment_txn_id) AS TotalOrders, ecommerce_website_name AS WebsiteName FROM ecomm WHERE ecommerce_website_name IS NOT null GROUP BY ecommerce_website_name")
    // websiteDF.show()

    // println("How much profit did each product generate")
    // val profitPNDF = spark.sql("SELECT sum(price) AS TotalProfit, product_name FROM ecomm GROUP BY product_name ORDER BY TotalProfit DESC")
    // profitPNDF.show()

    // println("How much profit did each product category generate")
    // val profitPCDF = spark.sql("SELECT sum(price) AS TotalProfit, product_category FROM ecomm GROUP BY product_category ORDER BY TotalProfit DESC")
    // profitPCDF.show()

    // println("How much profit does each country generate")
    // val profitCOUNTRYDF = spark.sql("SELECT sum(price) AS TotalProfit, country FROM ecomm GROUP BY country ORDER BY TotalProfit DESC")
    // profitCOUNTRYDF.show()

    // val sqlDF = spark.sql("SELECT country, count(order_id) FROM ecomm WHERE count(order_id) > 1000 GROUP BY country")
    // sqlDF.show()

   }
}