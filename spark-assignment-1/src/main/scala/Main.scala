import sparkAssignment1.FootballProcessEngine
import sparkAssignment2.PageCount
import sparkAssignment3.Movie

object Main extends App {

  import GlobalObject._

  val footballProcessEngine = new FootballProcessEngine
  println("Count of B.M. HomeTeam win: " + footballProcessEngine.getHomeTeamWin("Bayern Munich").count())

  val sortedData = footballProcessEngine.getMaxGoal.sortBy(x => -x._3)
  val finalOutput = sortedData.take(1).head
  println(finalOutput)

  val mostWin = footballProcessEngine.getMostWinBM("Bayern Munich")
  println("BM most win against Home Team: " + mostWin)

  println("Average goals: " + footballProcessEngine.getAvgGoal)

  println("Percentage of BM win: " + footballProcessEngine.getPercentageWin("Bayern Munich") + "%")

  val movie = new Movie

  println("Top ten highest rating movie: ")
  movie.getTopTenMovie.foreach(println)

  println("Top ten highest rated movie: ")
  movie.getTopRatedMovie.foreach(println)

  println("Most rating by User: " + movie.getMostRatedByUser)

  println("Total rating for sparkAssignment3.Movie 'Action': " + movie.getRatingForAction)

  println("Top ten rated by programmer: ")
  movie.getRatingByProgrammer.foreach(println)

  val pageCount = new PageCount

  println("Total of Page Count is: " + pageCount.getTotalPageCount)

  println("Top ten by hit : ")
  pageCount.getTopTen.foreach(println)

  println("English Page Count: " + pageCount.getEnglishCount)

  println("Count of Page Hit : " + pageCount.getPageHit)

  println("Count of AR type: " + pageCount.getARType)

  sparkContext.stop()
}
