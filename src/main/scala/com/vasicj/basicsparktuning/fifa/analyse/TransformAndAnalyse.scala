package com.vasicj.basicsparktuning.fifa.analyse

import org.apache.spark.sql.{DataFrame, SparkSession}

object TransformAndAnalyse {

  val defenders = List("RWB", "CB", "RB", "GK", "LB", "LWB")
  val midfielders = List("LM", "CAM", "LW", "CM", "RW", "CDM", "RM")
  val attackers = List("CF", "ST")

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

  def positions(df: DataFrame)(implicit ss: SparkSession): DataFrame = {
    import org.apache.spark.sql.functions._
    import ss.implicits._

    df.select(explode($"position"))
      .distinct()

  }

  def positionByNation(df: DataFrame)(implicit ss: SparkSession): List[Defensiveness] = {
    import org.apache.spark.sql.functions._
    import ss.implicits._

    val reducePositionsUDF =
      udf(
        (x: Array[String]) =>
          x.map {
            case s if defenders.contains(s)   => "defender"
            case s if midfielders.contains(s) => "midfielder"
            case s if attackers.contains(s)   => "attacker"
          }.distinct
      )
    ss.udf.register("reducePositions", reducePositionsUDF)

    val totalPerNation =
      broadcast(
        df.select("nationality")
          .groupBy("nationality")
          .agg(count("nationality").as("nationality_count"))
          .withColumnRenamed("nationality", "lnationality")
      )

    val positionPerNation = df
      .select("position", "nationality")
      .withColumn("position", reducePositionsUDF($"position"))
      .withColumn("position", explode($"position"))
      .groupBy("nationality", "position")
      .agg(count("position").as("position_count"))

    val stat = positionPerNation
      .join(
        totalPerNation,
        positionPerNation.col("nationality") === totalPerNation
          .col("lnationality"),
        "inner"
      )
      .withColumn(
        "defensiveness",
        ($"position_count" / $"nationality_count") * 100
      )

    stat
      .sort($"defensiveness".desc)
      .where("position = 'defender'")
      .select("nationality", "defensiveness")
      .as[Defensiveness]
      .collect()
      .toList

  }

  case class Stats(nationality: String, avgLevel: Double, avgPotential: Double)

  case class Defensiveness(nationality: String, defensiveness: Double)

}
