import org.apache.spark.{SparkConf, SparkContext}

object GlobalObject {

  val sparkConf = new SparkConf()
    .setAppName("Spark Page Count")
    .setMaster("local[*]")
    .set("spark.executor.memory", "1g")

  val sparkContext = new SparkContext(sparkConf)

  val Directory = "src/main/resources/"
  val moviesPath = Directory + "movies.dat"
  val ratingsPath = Directory + "ratings.dat"
  val usersPath = Directory + "users.dat"

}
