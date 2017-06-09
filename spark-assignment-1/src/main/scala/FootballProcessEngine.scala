package sparkAssignment1

import _root_.GlobalObject._
import org.apache.spark.rdd.RDD

import scala.math.abs

class FootballProcessEngine {
  def getFileData: RDD[String] = {
    sparkContext.textFile(footballPath)
  }

  def getHomeTeamWin(teamName: String): RDD[String] = {
    getFileData.filter { line =>
      val teamName = line.split(",")(2)
      val result = line.split(",")(6)
      teamName == teamName && result == "H"
    }
  }

  def getMaxGoal: RDD[(String, String, Int, String)] = {
    getFileData.map { line =>
      val teamData = line.split(",")
      val homeGoal = teamData(4).toInt
      val awayGoal = teamData(5).toInt
      val homeTeamName = teamData(2)
      val awayTeamName = teamData(3)
      val winTeam = teamData(6)
      val result = abs(homeGoal - awayGoal)
      (homeTeamName, awayTeamName, result, winTeam)
    }
  }

  def getMostWinBM(teamName: String): (String, Int, List[String]) = {
    val filteredRDD = getFileData.filter { line =>
      val array = line.split(",")
      val awayTeamName = array(3)
      val result = array(6)
      awayTeamName == teamName && result == "A"
    }
    val pairTeamRDD = filteredRDD.map { line =>
      val array = line.split(",")
      (array(2), array(3))
    }
    pairTeamRDD.groupByKey()
      .map(pairData => (pairData._1, pairData._2.size, pairData._2.toList.distinct))
      .sortBy(pairData => -pairData._2).take(1).head
  }

  def getAvgGoal: Map[Int, List[(String, Int)]] = {
    val homeTeamData = getFileData.map { line =>
      val array = line.split(",")
      val homeTeamName = array(2)
      val homeTeamGoal = array(4).toInt
      (homeTeamName, homeTeamGoal)
    }
    val homeGoalData = homeTeamData.groupByKey().map(x => (x._1, x._2.sum)).collect().toList
    val homeCount = homeGoalData.map(_._2).size
    val homeSum = homeGoalData.map(_._2).sum
    val homeAvg = homeSum / homeCount
    homeGoalData.groupBy(x => homeAvg)
  }

  def getPercentageWin(teamName: String): Int = {
    val filteredRDD = getFileData.filter { line =>
      val array = line.split(",")
      val homeTeamName = array(2)
      val awayTeamName = array(3)
      homeTeamName == teamName || awayTeamName == teamName
    }
    val mappedRDD = filteredRDD.map { line =>
      val array = line.split(",")
      val homeTeam = array(2)
      val awayTeam = array(3)
      if (homeTeam == teamName)
        (homeTeam, awayTeam, array(4).toInt, array(6))
      else
        (homeTeam, awayTeam, array(5).toInt, array(6))
    }

    val winRDD = mappedRDD.filter { case (homeTeam, awayTeam, goal, win) =>
      (homeTeam == teamName && win == "H") || (awayTeam == teamName && win == "A")
    }.map(_._3)

    val lossRDD = mappedRDD.filter { case (homeTeam, awayTeam, goal, win) =>
      (homeTeam != teamName && win != "H") || (awayTeam != teamName && win != "A")
    }.map(_._3)

    val winSum = winRDD.collect().sum
    val lossSum = lossRDD.collect().sum
    val total = winSum + lossSum
    (winSum * 100) / total
  }

}
