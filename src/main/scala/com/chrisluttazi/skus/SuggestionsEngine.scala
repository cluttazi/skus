package com.chrisluttazi.skus

import com.chrisluttazi.skus.model.{ Sku, SkuDifferences, SuggestionsEngineT }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.util.Try

class SuggestionsEngine extends Serializable with SuggestionsEngineT {

  /**
   * Creates a features array, this will fail in case the string is not formatted as in the file
   *
   * @param row : a row, for example: [att-a-7,att-b-3,att-c-10,att-d-10,att-e-15,att-f-11,att-g-2,att-h-7,att-i-5,att-j-14]
   * @return
   */
  private def convertToArray(row: String): Array[Int] = {
    val rowArray: Array[String] = row.split(",")
    val array: Array[Int] = new Array[Int](rowArray.length)
    for ((a, i) <- rowArray.view.zipWithIndex) {
      val b: Array[String] = a.split("-")
      val index: Try[Int] = Try(b(1).charAt(0).toInt - 'a')
      val value: Try[Int] = Try(b(2).toInt)
      if (index.isSuccess && value.isSuccess) array(index.get) = value.get
    }
    array
  }

  /**
   * Calculates the absolute difference between two arrays
   *
   * @param s1 : An attributes array
   * @param s2 : An attributes array
   * @return
   */
  private def calculateDifference(s1: Array[Int], s2: Array[Int]): Array[Int] = {
    if (s1.length >= s2.length) {
      val length: Int = s1.length
      val array: Array[Int] = new Array[Int](length)
      for ((e, i) <- s1.view.zipWithIndex) array(i) = Math.abs(s2(i) - e)
      array
    } else calculateDifference(s2, s1)
  }

  override def compare(sku1: Sku, sku2: Sku): SkuDifferences = {
    if (sku1.attributes.length >= sku2.attributes.length) {
      val diff: Array[Int] = calculateDifference(sku1.attributes, sku2.attributes)
      var coef: BigInt = Math.pow(100, sku1.attributes.length).toInt
      var breaker: BigInt = 0
      for (n <- diff) {
        breaker += coef * n
        coef /= 100
      }
      SkuDifferences(sku2, diff.sum, breaker)
    } else compare(sku2, sku1)

  }

  override def parseSku(ds: DataFrame): RDD[Sku] =
    ds.rdd.map(row =>
      Sku(row(1).toString, convertToArray(row(0).toString)))

  override def createDifferencesRDD(sku: Sku, rdd: RDD[Sku]): RDD[SkuDifferences] =
    rdd
      .map(compare(sku, _))
      .sortBy(sd => (sd.difference, sd.breaker))

  override def getBestRecommendations(sku: Sku, rdd: RDD[Sku], n: Int): Array[SkuDifferences] =
    createDifferencesRDD(sku, rdd).take(n)

}

object SuggestionsEngine extends SuggestionsEngine
