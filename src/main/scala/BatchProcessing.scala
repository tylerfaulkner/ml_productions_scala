import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import java.net.URI

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
    val data = spark.read.json("s3a://log-files/*")
    val spamEmails = data.where(col("email_id") === "spam").select(col("email_id"), col("label"))
    println("Getting Postgres Data...")
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://127.0.0.1:5432/email_ingestion")
      .option("dbtable", "emails")
      .option("user", "ingestion_service")
      .option("password", "puppet-soil-SWEETEN")
      .load()

    val needed_data = jdbcDF.select(col("email_id"), col("email_object"))

    val leftJoin = needed_data.join(spamEmails, needed_data("email_id") === spamEmails("email_id"), "left_outer")

    leftJoin.show()

  }
}
