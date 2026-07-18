package com.chrisluttazi.skus

import com.chrisluttazi.skus.model.{ Sku, SkuDifferences }
import com.chrisluttazi.skus.spark.Spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.{ Logger, LoggerFactory }

class SuggestionsEngineSpec extends AnyWordSpec with Matchers {
  val log: Logger = LoggerFactory.getLogger(getClass)

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

    "weigh differences in trailing attributes of wide skus (regression: breaker coefficient overflow)" in {
      // With 10 attributes the initial coefficient is 100^10, which overflowed
      // Int and zeroed the weights of every attribute after the fifth, so
      // differences in trailing attributes were invisible to the tie-breaker.
      val pivot = Sku("pivot", Array.fill(10)(0))
      val diffAtLast = Sku("last", Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 2))
      val diffAtNinth = Sku("ninth", Array(0, 0, 0, 0, 0, 0, 0, 0, 2, 0))

      val cLast: SkuDifferences = SuggestionsEngine.compare(pivot, diffAtLast)
      val cNinth: SkuDifferences = SuggestionsEngine.compare(pivot, diffAtNinth)

      cLast.difference should ===(cNinth.difference)
      // weights are 100^(n-i): position 9 -> 2 * 100^1, position 8 -> 2 * 100^2
      cLast.breaker should ===(BigInt(200))
      cNinth.breaker should ===(BigInt(20000))
      cLast.breaker should be < cNinth.breaker
    }

    "be able to create suggestions" in {

      val path: String = this.getClass.getResource("/input.json").getPath

      val ds: DataFrame = Spark.session.read.json(path)

      //aggregate the order
      val rdd: RDD[Sku] = SuggestionsEngine.parseSku(ds)
      // take one and find top matches
      val sku: Sku = rdd.take(1)(0)
      //calculate the difference
      val rddDifferences: RDD[SkuDifferences] = SuggestionsEngine.createDifferencesRDD(sku, rdd)
      rddDifferences.count() should ===(rdd.count())
      //grab best skus
      val recommendations: Array[SkuDifferences] = SuggestionsEngine.getBestRecommendations(sku, rdd, 15)
      recommendations should have length 15
      // the pivot sku is its own closest match (zero difference)
      recommendations.head.sku.sku should ===(sku.sku)
      recommendations.head.difference should ===(0)

      log.info(s"Comparison sku ${sku.sku} attributes ${sku.attributes.mkString(" ")}")
      recommendations.foreach(s => {
        log.info(s"sku ${s.sku.sku} diff ${s.difference} breaker ${s.breaker}")
        log.info(s"attributes ${s.sku.attributes.mkString(" ")}")
      })
    }
  }
}
