package com.vasicj.basicsparktuning.fifa.ingest

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class FifaIngestTest extends AnyFunSuite with SharedSparkContext {

  test("read file with the correct schema") {
    implicit val testSession =
      SparkSession
        .builder()
        .appName("fifa test")
        .config("spark.master", "local")
        .getOrCreate()
    val testSchema = StructType(
      Seq(
        StructField("player_id", StringType, true),
        StructField("name", StringType, true),
        StructField("nationality", StringType, true),
        StructField("position", ArrayType(StringType), true),
        StructField("overall", IntegerType, false),
        StructField("age", IntegerType, false),
        StructField("hits", IntegerType, false),
        StructField("potential", IntegerType, false),
        StructField("team", StringType, true)
      )
    )

    val result =
      IngestIntoDF.ingest("src/test/resources/TestFifa.csv")

    assert(result.schema == testSchema)
  }
}
