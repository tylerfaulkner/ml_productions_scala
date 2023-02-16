import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.net.URI
import java.time.LocalDateTime

object BatchProcessing {
  def main(args: Array[String]): Unit = {
    println("Starting")
    val spark = SparkSession
      .builder
      // set number of cores to use in []
      .master("local[2]")
      .appName("BatchProcessing")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "minioadmin")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "minioadmin")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://127.0.0.1:9000")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")

    println("Getting Minio Files")
    val data = spark.read.parquet("s3a://log-files/log.parquet")
    val spamEmails = data.where(col("label") === "spam").select(col("email_id"), col("label"))
    println("Getting Postgres Data...")
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://127.0.0.1:5432/email_ingestion")
      .option("dbtable", "emails")
      .option("user", System.getenv("POSTGRES_USERNAME"))
      .option("password", System.getenv("POSTGRES_PASSWORD"))
      .load()

    val needed_data = jdbcDF.select(col("email_id"), col("email_object"))

    val struct = StructType(Seq(
      StructField("to", StringType),
      StructField("from", StringType),
      StructField("subject", StringType),
      StructField("body", StringType)
    ))

    val expanded_data = needed_data.withColumn("email_object", from_json(col("email_object"), struct))
      .select(col("email_id"), col("email_object.*"))

    expanded_data.show()

    val leftJoin = expanded_data.join(spamEmails, Seq("email_id"), "left")

    val cleaned = leftJoin.na.fill("ham", Seq("label"))
    val date_time = LocalDateTime.now()
    val folder_name = String.format("data_(%d_%d_%d)_(%d_%d_%d)", date_time.getMonthValue(), date_time.getDayOfMonth(), date_time.getYear(), date_time.getHour(), date_time.getMinute(), date_time.getSecond())
    cleaned.show()
    cleaned.write
      .option("fs.s3a.committer.name", "partitioned")
      .json(String.format("s3a://training-data/%s", folder_name))
  }
}
