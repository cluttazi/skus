package com.chrisluttazi.skus.model

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

trait SuggestionsEngineT {
  /**
   * Returns an int based on the differences, the lower the breaker, the better
   *
   * @param sku1 : SE
   * @param sku2 : SE
   * @return
   */
  def compare(sku1: Sku, sku2: Sku): SkuDifferences

  /**
   * Creates an [[RDD]] of type [[Sku]] based on a [[DataFrame]]
   *
   * @param ds : input dataset
   * @return
   */
  def parseSku(ds: DataFrame): RDD[Sku]

  /**
   * Creates [[RDD]] of type [[SkuDifferences]] based on one sku to pivot
   *
   * @param sku : Sku used to Pivot
   * @param rdd : Rdd with other skus to compare
   * @return
   */
  def createDifferencesRDD(sku: Sku, rdd: RDD[Sku]): RDD[SkuDifferences]

  /**
   * Gets the best N recommendations
   *
   * @param sku : Sku used to Pivot
   * @param rdd : Rdd with other skus to compare
   * @param n   : Number of recommendations
   * @return
   */
  def getBestRecommendations(sku: Sku, rdd: RDD[Sku], n: Int): Array[SkuDifferences]
}
