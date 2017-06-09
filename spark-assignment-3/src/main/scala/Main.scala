import GlobalObject._

object Main extends App {

  val movie = new Movie

  println("Top ten highest rating movie: ")
  movie.getTopTenMovie.foreach(println)

  println("Top ten highest rated movie: ")
  movie.getTopRatedMovie.foreach(println)

  println("Most rating by User: " + movie.getMostRatedByUser)

  println("Total rating for Movie 'Action': " + movie.getRatingForAction)

  println("Top ten rated by programmer: ")
  movie.getRatingByProgrammer.foreach(println)

  sparkContext.stop()
}
