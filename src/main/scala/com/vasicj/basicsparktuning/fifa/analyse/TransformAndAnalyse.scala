package com.vasicj.basicsparktuning.fifa.analyse

import org.apache.spark.sql.{DataFrame, SparkSession}

object TransformAndAnalyse {

  def stats(df: DataFrame)(implicit ss: SparkSession): List[Stats] = {
    import ss.implicits._

    df.select("potential", "overall", "nationality")
      .groupBy("nationality")
      .agg("overall" -> "avg", "potential" -> "avg")
      .toDF("nationality", "avgLevel", "avgPotential")
      .as[Stats]
      .collect()
      .toList

  }

  case class Stats(nationality: String, avgLevel: Double, avgPotential: Double)
}
