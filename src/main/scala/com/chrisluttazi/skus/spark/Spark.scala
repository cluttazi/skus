package com.chrisluttazi.skus.spark

import org.apache.spark.sql.SparkSession

trait Spark {
  lazy val session: SparkSession = SparkSession.builder
    .master("local")
    .appName("skus")
    .getOrCreate()

}

object Spark extends Spark