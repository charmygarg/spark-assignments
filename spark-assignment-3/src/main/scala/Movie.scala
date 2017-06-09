import GlobalObject._
import org.apache.spark.rdd.RDD

class Movie {

  val movieData = sparkContext.textFile(moviesPath)
  val ratingData = sparkContext.textFile(ratingsPath)
  val userData = sparkContext.textFile(usersPath)

  val ratingInfo: RDD[(Long, Int)] = getRatingPairData
  val movieInfo: RDD[(Long, String)] = getMoviePairData
  val userInfo: RDD[(Int, String)] = getUserData
  val userRatingInfo: RDD[(Int, Int)] = getUserRating
  val userOccupation: RDD[String] = getUserOccupation
  val userIdAndOccupation: RDD[(Int, Int)] = getUserIdAndOccupation
  val info: RDD[(Int, (Long, Int))] = getRating

  def getTopTenMovie: Array[(String, Int)] = {
    val joinData = movieInfo.join(ratingInfo).groupBy(_._2._1)
    val groupedData = joinData.map(data => (data._1, data._2.map(_._2._2).sum / data._2.map(_._2._2).size))
    groupedData.sortBy(-_._2).take(10)
  }

  def getMoviePairData: RDD[(Long, String)] = {
    movieData.map { line =>
      val array = line.split("::")
      val movieId = array(0).toLong
      val movieTitle = array(1)
      (movieId, movieTitle)
    }
  }

  def getRatingPairData: RDD[(Long, Int)] = {
    ratingData.map { line =>
      val array = line.split("::")
      val ratingMovieId = array(1).toLong
      val rating = array(2).toInt
      (ratingMovieId, rating)
    }
  }

  def getTopRatedMovie: Array[(String, Int)] = {
    val joinData = movieInfo.join(ratingInfo).groupBy(_._2._1)
    val dataCount = joinData.map(data => (data._1, data._2.map(_._2._2).sum))
    dataCount.sortBy(-_._2).take(10)
  }

  def getUserData: RDD[(Int, String)] = {
    userData.map { line =>
      val array = line.split("::")
      val userId = array(0).toInt
      val gender = array(1)
      (userId, gender)
    }
  }

  def getUserRating: RDD[(Int, Int)] = {
    ratingData.map { line =>
      val array = line.split("::")
      val ratingUserId = array(0).toInt
      val rating = array(2).toInt
      (ratingUserId, rating)
    }
  }

  def getMostRatedByUser: (Int, Int) = {
    val joinData = userRatingInfo.join(userInfo).groupBy(_._1)
    val userCount = joinData.map(data => (data._1, data._2.map(_._1).size))
    userCount.sortBy(-_._2).take(1).head
  }

  def getMovieWithTitle: RDD[String] = {
    movieData.filter { line =>
      val genre = line.split("::")(2)
      genre.toLowerCase.contains("action")
    }
  }

  def getRatingForAction: Long = {
    ratingInfo.join(movieInfo).count()
  }

  def getUserOccupation: RDD[String] = {
    userData.filter {line =>
      val occupation = line.split("::")(3).toInt
      occupation == 12
    }
  }

  def getUserIdAndOccupation: RDD[(Int, Int)] = {
    userOccupation.map { line =>
      val array = line.split("::")
      val userId = array(0).toInt
      val occupation = array(3).toInt
      (userId, occupation)
    }
  }

  def getRating: RDD[(Int, (Long, Int))] = {
    ratingData.map { line =>
      val array = line.split("::")
      val ratingUserId = array(0).toInt
      val movieId = array(1).toLong
      val rating = array(2).toInt
      (ratingUserId, (movieId, rating))
    }
  }

  def getRatingByProgrammer: Array[(String, Int)] = {
    val movieIdAndRating = userIdAndOccupation.join(info).map(data => (data._2._2._1, data._2._2._2))
    val joinData = movieInfo.join(movieIdAndRating).groupBy(_._2._1)
    val groupedData = joinData.map(data => (data._1, data._2.map(_._2._2).sum / data._2.map(_._2._2).size))
    groupedData.sortBy(-_._2).take(10)
  }

}
