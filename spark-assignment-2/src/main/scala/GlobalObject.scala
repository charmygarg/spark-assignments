import org.apache.spark.{SparkConf, SparkContext}

object GlobalObject {

  val sparkConf = new SparkConf()
    .setAppName("Spark Page Count")
    .setMaster("local[*]")
    .set("spark.executor.memory", "1g")

  val sparkContext = new SparkContext(sparkConf)

  val filePath = "src/main/resources/pagecounts-20151201-220000.txt"
}
