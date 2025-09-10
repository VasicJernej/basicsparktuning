package com.vasicj.basicsparktuning.fifa.analyse

import com.holdenkarau.spark.testing.SharedSparkContext
import com.vasicj.basicsparktuning.fifa.ingest.IngestIntoDF
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class TramsformAndAnalyseTest extends AnyFunSuite with SharedSparkContext {

  test("successfully calculate stats") {
    implicit val testSession =
      SparkSession
        .builder()
        .appName("fifa test")
        .config("spark.master", "local")
        .getOrCreate()

    val result =
      TransformAndAnalyse.stats(
        IngestIntoDF.ingest("src/test/resources/TestFifa.csv")
      )

    assert(result.head.avgLevel == 89)
    assert(result.head.avgPotential == 90)
    assert(result.head.nationality == "Senegal")
  }

  test("positions will return the distinct of all positions in the dataset") {
    implicit val testSession =
      SparkSession
        .builder()
        .appName("fifa test")
        .config("spark.master", "local")
        .getOrCreate()

    val testPositions =
      List("LM", "CAM", "LW", "GK", "CM", "RW", "CDM", "CB", "CF", "ST")

    val result =
      TransformAndAnalyse.positions(
        IngestIntoDF.ingest("src/test/resources/TestFifa.csv")
      )

    assert(
      result
        .take(10)
        .toList
        .map(x => x.toString().tail.dropRight(1)) == testPositions
    )
  }

  test(
    "positionByNationality should return the most defensive nationalities first"
  ) {
    implicit val testSession =
      SparkSession
        .builder()
        .appName("fifa test")
        .config("spark.master", "local")
        .getOrCreate()

    val result =
      TransformAndAnalyse
        .positionByNation(
          IngestIntoDF.ingest("src/test/resources/TestFifa.csv")
        )

    val testPositionsByCountry = "Slovenia"

    assert(result.head.nationality == testPositionsByCountry)
  }
}
