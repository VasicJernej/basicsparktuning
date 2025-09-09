package com.vasicj.basicsparktuning.fifa.store

import com.vasicj.basicsparktuning.fifa.analyse.TransformAndAnalyse.Stats
import org.apache.spark.sql.SparkSession

object StoreIntoFile {

  def store(stats: List[Stats], path: String)(implicit ss: SparkSession): Unit =
    ss.createDataFrame(stats)
      .rdd
      .saveAsTextFile(path)
}
