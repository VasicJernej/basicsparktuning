package com.vasicj.basicsparktuning.fifa.store

import com.holdenkarau.spark.testing.SharedSparkContext
import com.vasicj.basicsparktuning.fifa.analyse.TransformAndAnalyse.Stats
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Try

class FifaStoreTest extends AnyFunSuite with SharedSparkContext {

  test("store stats into file") {
    implicit val ss =
      SparkSession
        .builder()
        .appName("fifa test")
        .config("spark.master", "local")
        .getOrCreate()

    val stats =
      List(Stats("Slovenia", 82.4, 82.7))

    // Something less IO intensive would be better
    val beforeTestState = Try {
      scala.io.Source.fromFile("src/test/resources/outputFile/_SUCCESS")
    }.isSuccess

    if (beforeTestState == true) {
      import java.io.File

      import org.apache.commons.io.FileUtils
      FileUtils.deleteDirectory(new File("src/test/resources/outputFile"))
    }

    StoreIntoFile
      .store(stats, "src/test/resources/outputFile")

    val res = Try {
      scala.io.Source.fromFile("src/test/resources/outputFile/_SUCCESS")
    }.isSuccess

    assert(res == true)

  }
}
