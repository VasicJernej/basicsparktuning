package com.vasicj.basicsparktuning.fifa.store

import com.vasicj.basicsparktuning.fifa.analyse.TransformAndAnalyse.{Defensiveness, Stats}
import org.apache.spark.sql.SparkSession

object StoreIntoFile {

  def store(stats: List[Stats], path: String)(implicit ss: SparkSession): Unit =
    ss.createDataFrame(stats)
      .rdd
      .saveAsTextFile(path)

  def storeDefensiveness(stats: List[Defensiveness], path: String)(implicit ss: SparkSession): Unit =
    ss.createDataFrame(stats)
      .rdd
      .saveAsTextFile(path)
}
