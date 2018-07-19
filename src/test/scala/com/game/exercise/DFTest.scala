package com.game.exercise

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons, SharedSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.mutable


class DFTest extends FunSuite with SharedSparkContext with DataFrameSuiteBase {

  test("DF Transformation Tests") {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val input = new mutable.HashMap[String, DataFrame]

    /* Populate the output map */

    input += ("TEAMS" -> sc.parallelize(Seq(("PLAYER1", "TEAM1"), ("PLAYER2", "TEAM2")))
      .toDF("player", "team"))

    input += ("SCORES" -> sc.parallelize(
      Seq(("PLAYER1", "DAY1", 3.45), ("PLAYER2", "DAY1", 6.78)))
      .toDF("player", "day", "score"))

    val expectedDF = sc.parallelize(Seq(("TEAM2", 6.78), ("PLAYER2", 6.78))).toDF("winner", "score")

    val sparkTestSession = sqlCtx.sparkSession

    val resultDF = new DFRunner(sparkTestSession).transform(input.toMap)

    assertDataFrameEquals(expectedDF, resultDF)

  }

}