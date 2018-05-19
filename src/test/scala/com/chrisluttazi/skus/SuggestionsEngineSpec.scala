package com.chrisluttazi.skus

import com.chrisluttazi.skus.model.{ Sku, SkuDifferences }
import com.chrisluttazi.skus.spark.Spark
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ Matchers, WordSpec }

class SuggestionsEngineSpec extends WordSpec with Matchers with ScalaFutures {
  val log = LogManager.getRootLogger

  "SuggestionsEngine" should {

    /**
     * Example 1:
     * {"sku":"sku-1","attributes": {"att-a": "a1", "att-b": "b1", "att-c": "c1"}} is more similar to
     * {"sku":"sku-2","attributes": {"att-a": "a2", "att-b": "b1", "att-c": "c1"}} than to
     * {"sku":"sku-3","attributes": {"att-a": "a1", "att-b": "b3", "att-c": "c3"}}
     * *
     * Example 2:
     * {"sku":"sku-1","attributes":{"att-a": "a1", "att-b": "b1"}} is more similar to
     * {"sku":"sku-2","attributes":{"att-a": "a1", "att-b": "b2"}} than to
     * {"sku":"sku-3","attributes":{"att-a": "a2", "att-b": "b1"}}
     *
     */

    val sku1 = Sku("sku-1", Array(1, 1, 1))
    val sku2 = Sku("sku-2", Array(2, 1, 1))
    val sku3 = Sku("sku-3", Array(1, 3, 3))
    val sku4 = Sku("sku-4", Array(1, 1))
    val sku5 = Sku("sku-5", Array(1, 2))
    val sku6 = Sku("sku-6", Array(2, 1))

    "be able to compare skus" in {
      val c12: SkuDifferences = SuggestionsEngine.compare(sku1, sku2)
      val c13: SkuDifferences = SuggestionsEngine.compare(sku1, sku3)
      c12.difference should be < c13.difference
      c12.breaker should be > c13.breaker

      val c45: SkuDifferences = SuggestionsEngine.compare(sku4, sku5)
      val c46: SkuDifferences = SuggestionsEngine.compare(sku4, sku6)

      c45.difference should ===(c46.difference)
      c45.breaker should be < c46.breaker
    }

    "be able to create suggestions" in {

      val path: String = this.getClass.getResource("/input.json").getPath

      val ds: DataFrame = Spark.session.read.json(path)

      log.info(s"${ds.printSchema}")

      //aggregate the order
      val rdd: RDD[Sku] = SuggestionsEngine.parseSku(ds)
      // take one and find top matches
      val sku: Sku = rdd.take(1)(0)
      //calculate the difference
      val rddDifferences: RDD[SkuDifferences] = SuggestionsEngine.createDifferencesRDD(sku, rdd)
      log.info(s"${rddDifferences.take(10)}")
      //grab best skus
      val recommendations: Array[SkuDifferences] = SuggestionsEngine.getBestRecommendations(sku, rdd, 15)

      def printInt(i: Int): String = s" $i "

      log.info(s"Comparison sku ${sku.sku} attributes " +
        s"${sku.attributes.foldLeft("") { (acc, i) => acc + s"${i.toString} " }}}")
      recommendations.foreach(s => {
        log.info(s"sku ${s.sku.sku} diff ${s.difference} breaker ${s.breaker}")
        log.info(s"attributes ${s.sku.attributes.foldLeft("") { (acc, i) => acc + s"${i.toString} " }}")
      })
    }
  }
}
