package com.game.exercise

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable

case class Teams(name: String, team: String)

class DFRunner(val spark: SparkSession) {
  /**
    * @param input a map of the file paths to extract. The key is a [[String]] alias, the value is the [[String]] path of the file to extract.
    * @return a map of [[DataFrame]] per each input
    */
  def extract(input: Map[String, String]): Map[String, DataFrame] = {

    /* Define Schemas */

    // define fields
    val player = StructField("player", DataTypes.StringType)
    val team = StructField("team", DataTypes.StringType)

    val day = StructField("day", DataTypes.StringType)
    val score = StructField("score", DataTypes.DoubleType)

    // define schemas
    val teamSchema = StructType(Array(player, team))
    val scoreSchema = StructType(Array(player, day, score))

    val output = new mutable.HashMap[String, DataFrame]

    /* Populate the output map */

    // TBD: Generalize the populating of map using the keys.

    output += "TEAMS" -> spark.read.schema(teamSchema).csv(input("TEAMS"))
    output += "SCORES" -> spark.read.schema(scoreSchema).csv(input("SCORES"))

    output.toMap

  }

  /**
    * @param extracted a map of [[DataFrame]] indexed by a [[String]] alias
    * @return
    */
  def transform(extracted: Map[String, DataFrame]): DataFrame = {

    /*
     * Avoiding SQL syntax and use the DataFrame API as much as possible.
     */

    val teamsDF = extracted("TEAMS")
    val scoresDF = extracted("SCORES")

    require(teamsDF != null, "Teams Data is missing!!! App cannot continue without Team Data")
    require(scoresDF != null, "Scores Data is missing!!! App cannot continue without Scores Data")

    import org.apache.spark.sql.functions._
    import spark.implicits._

    /* Player Scores */

    // Cache the players along with their scores formatted for 2 decimal places
    val playerCache = scoresDF.groupBy($"player").agg( sum($"score").alias(("score")))
      .map(row => (row(0).asInstanceOf[String], row(1).asInstanceOf[Double].formatted("%.2f").toDouble))
      .toDF("player", "score")
      .cache()

    // Retrieve the max score of the player
    val maxPlayerScore = playerCache.agg( max($"score")).first().getAs[Double](0)

    // Filter players with maximum scores
    val playerScoresMax = playerCache.filter($"score" === maxPlayerScore)

    /* Join Teams with Scores to retrieve the team with high points */

    // cache the frequently used join of two dataframes
    val joinDFCache = scoresDF.as("t1").join(teamsDF.as("t2"), $"t1.player" === $"t2.player")
      .select($"t2.team", $"t1.score").groupBy($"team").agg(sum($"score").alias("score"))
      .map(row => (row(0).asInstanceOf[String], row(1).asInstanceOf[Double].formatted("%.2f").toDouble))
      .toDF("team", "score").cache()

    // retrieve the maximum score to use in future with multiple teams who may share the same max score
    val maxTeamScore = joinDFCache.agg( max($"score")).first().getAs[Double](0)

    // retrieve only the elements that have the maximum score
    val teamsWithMaxScore = joinDFCache.filter($"score" === maxTeamScore)

    // return a union that represents the winning player(s) and team(s).
    teamsWithMaxScore.union(playerScoresMax).toDF("winner", "score")

  }

  /**
    * @param transformed the [[DataFrame]] to store as a file
    * @param path        the path to save the output file
    */
  def load(transformed: DataFrame, path: String): Unit = {
    transformed.write.format("csv").save(path)
  }
}
