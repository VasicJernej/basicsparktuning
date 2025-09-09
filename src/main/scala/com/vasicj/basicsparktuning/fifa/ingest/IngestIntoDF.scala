package com.vasicj.basicsparktuning.fifa.ingest

import org.apache.spark.sql.{DataFrame, SparkSession}

object IngestIntoDF {

  def ingest(file: String)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    ss.read
      .option("delimiter", ";")
      .csv(file)
      .map(
        line =>
          FifaSchema(
            line(0).toString,
            line(1).toString,
            line(2).toString,
            line(3).toString.split('|'),
            line(4).toString.trim.toInt,
            line(5).toString.trim.toInt,
            line(6).toString.trim.toInt,
            line(7).toString.trim.toInt,
            line(8).toString
        )
      )
      .toDF()

  }

  case class FifaSchema(player_id: String,
                        name: String,
                        nationality: String,
                        position: Array[String],
                        overall: Int,
                        age: Int,
                        hits: Int,
                        potential: Int,
                        team: String)

}
