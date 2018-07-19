package com.game.exercise

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class RDDTest extends FunSuite with SharedSparkContext with RDDComparisons {

  test("RDD Transformation Tests") {

    val input = new mutable.HashMap[String, RDD[String]]

    input += ("TEAMS" -> sc.parallelize(Seq( ("PLAYER1, TEAM1"), ("PLAYER2, TEAM2") )))
    input += ("SCORES" -> sc.parallelize(Seq( ("PLAYER1, DAY1, 3.45"), ("PLAYER2, DAY1, 6.78") )))

    val expectedRDD = sc.parallelize( Seq( ("TEAM2", 6.78), ("PLAYER2", 6.78) ) )

    val resultRDD = new RDDRunner(sc).transform(input.toMap)

    assertRDDEquals(expectedRDD, resultRDD)

  }

}