import org.apache.spark.sql.SparkSession
import io.minio.MinioClient
import io.minio.ListObjectsArgs

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

    println("Getting Minio")
    val minioClient = MinioClient.builder()
      .endpoint(sys.env("MINIO_URL"))
      .credentials(sys.env("SPARK_MINIO_ACCESS_KEY"), sys.env("SPARK_MINIO_SECRET_KEY"))
      .build()
    println("Getting Minio Objects")
    val objects = minioClient.listObjects(ListObjectsArgs.builder().bucket("log-files").build())
    val objects_iter = objects.iterator()
    println("Iterating over objects")
    while (objects_iter.hasNext) {
      val objectInfo = objects_iter.next().get()
      println(objectInfo.objectName())
      println(objectInfo)
      println(objectInfo.toString)
    }

  }
}
