package com.game.exercise

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class RDDRunner(val context: SparkContext) {
  /**
    * @param input a map of the file paths to extract. The key is a [[String]] alias, the value is the [[String]] path of the file to extract.
    * @return a map of [[RDD]] per each input
    */
  def extract(input: Map[String, String]): Map[String, RDD[String]] = {

    val output = new mutable.HashMap[String, RDD[String]]

    for ((k, p) <- input) {

      output += (k -> context.textFile(p))

    }

    output.toMap

  }

  /**
    * @param extracted a map of [[RDD]] indexed by a [[String]] alias
    * @return
    */
  def transform(extracted: Map[String, RDD[String]]): RDD[(String, Double)] = {

    /* Get the Rdds from Map */
    val teamsRdd = extracted("TEAMS")
    val scoresRdd = extracted("SCORES")

    require(teamsRdd != null, "Teams Data is missing!!! App cannot continue without Team Data")
    require(scoresRdd != null, "Scores Data is missing!!! App cannot continue without Scores Data")

    val playerList = scoresRdd.map(x => (x.split(",")(0).trim,
      x.split(",")(2).trim.toDouble
    ))

    // Cache the players RDD
    val playerRddCache = playerList.reduceByKey(_ + _).map(x => (x._1, x._2.formatted("%.2f").toDouble)).cache()

    // Retrieve the max points from the cache
    val playerMaxPoints = playerRddCache.values.max

    // Retrieve players with max points
    val playersWithMaxPoints = playerRddCache.filter { case (_, v) => v == playerMaxPoints }

    /* Work on building teams with high performance */
    val teams = teamsRdd.map(x => (
      x.split(",")(0).trim,
      x.split(",")(1).trim
    )
    )

    // build a teams rdd cache joining teams with players
    val teamsRddCache = teams.join(playerRddCache).map(x => (x._2._1, x._2._2)).reduceByKey(_+_).cache()

    // find the max points a team could reach to sort out existence of multiple teams
    val maxTeamPoints = teamsRddCache.values.max

    // filter the list and retrieve the teams that meet high performance criteria
    val teamsList = teamsRddCache.filter{ case (_, v) => v == maxTeamPoints }

    // return a union of the final desired result
    teamsList.union(playersWithMaxPoints)

  }

  /**
    * @param transformed the [[RDD]] to store as a file
    * @param path        the path to save the output file
    */
  def load(transformed: RDD[(String, Double)], path: String): Unit = {
    transformed.saveAsTextFile(path)
  }

}
