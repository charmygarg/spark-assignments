
object Main extends App {

  import GlobalObject._

  val pageCount = new PageCount

  println("Total of Page Count is: " + pageCount.getTotalPageCount)

  println("Top ten by hit : ")
  pageCount.getTopTen.foreach(println)

  println("English Page Count: " + pageCount.getEnglishCount)

  println("Count of Page Hit : " + pageCount.getPageHit)

  println("Count of AR type: " + pageCount.getARType)

  sparkContext.stop()
}
