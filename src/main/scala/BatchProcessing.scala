import org.apache.spark.sql.SparkSession
import io.minio.MinioClient
import io.minio.ListObjectsArgs

object BatchProcessing {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      // set number of cores to use in []
      .master("local[2]")
      .appName("BatchProcessing")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val minioClient = MinioClient.builder()
      .endpoint("http://127.0.0.1:9000")
      .credentials(sys.env("MINIO_ACCESS_KEY"), sys.env("MINIO_SECRET_KEY"))
      .build()
    val objects = minioClient.listObjects(ListObjectsArgs.builder().bucket("log-files").build())
    val objects_iter = objects.iterator()
    while (objects_iter.hasNext()) {
      val objectInfo = objects_iter.next()
      println(objectInfo.toString)
    }

  }
}
