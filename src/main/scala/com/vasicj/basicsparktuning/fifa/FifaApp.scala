package com.vasicj.basicsparktuning.fifa

import com.vasicj.basicsparktuning.fifa.analyse.TransformAndAnalyse
import com.vasicj.basicsparktuning.fifa.ingest.IngestIntoDF
import com.vasicj.basicsparktuning.fifa.store.StoreIntoFile
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object FifaApp extends App {
  val conf = new SparkConf().setMaster("local")
  implicit val ss = SparkSession.builder().config(conf).getOrCreate()
  val path = "datasets/fifa/FIFA-21Complete.csv"
  val storePath = "datasets/fifa/outputFile"
  val e: DataFrame = IngestIntoDF.ingest(path)
  val t = TransformAndAnalyse.stats(e)
  StoreIntoFile.store(t, storePath)
}
