import org.apache.spark.sql.SparkSession

class BatchProcessing {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      // set number of cores to use in []
      .master("local[2]")
      .appName("BatchProcessing")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }
}
