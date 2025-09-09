package com.vasicj.basicsparktuning.fifa.analyse

import com.holdenkarau.spark.testing.SharedSparkContext
import com.vasicj.basicsparktuning.fifa.ingest.IngestIntoDF
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class TramsformAndAnalyseTest extends AnyFunSuite with SharedSparkContext {

  test("read file with the correct schema") {
    implicit val testSession =
      SparkSession
        .builder()
        .appName("fifa test")
        .config("spark.master", "local")
        .getOrCreate()

    val result =
      TransformAndAnalyse.stats(IngestIntoDF.ingest("src/test/resources/TestFifa.csv"))

    assert(result.head.avgLevel == 89)
    assert(result.head.avgPotential == 90)
    assert(result.head.nationality == "Senegal")
  }
}
