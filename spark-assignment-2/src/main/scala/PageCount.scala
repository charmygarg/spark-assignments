import GlobalObject._
import org.apache.spark.rdd.RDD

class PageCount {

  def getFileData: RDD[String] = {
    sparkContext.textFile(filePath)
  }

  def getTotalPageCount: Double = {
    getFileData.map(_.split(" ")(3).toLong).sum()
  }

  def getTopTen: Array[Long] = {
    getFileData.map(_.split(" ")(3).toLong).sortBy(-_).take(10)
  }

  def getEnglishCount: Long = {
    getFileData.filter(_.split(" ")(1).contains("/en/")).count()
  }

  def getPageHit: Long = {
    val filteredData = getFileData.map { line =>
      val array = line.split(" ")
      (array(1).toLowerCase, array(3).toLong)
    }
    val pairData = filteredData.groupByKey().map(data => (data._1, data._2.sum))
    pairData.filter(_._2 > 200000).count()
  }

  def getARType = {
    val filteredData = getFileData.filter { line =>
      val category = line.split(" ")(0)
      category == "ar"
    }
    filteredData.map(_.split(" ")(3).toLong).sum()
  }

}
