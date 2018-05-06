package com.chrisluttazi.skus

import com.chrisluttazi.skus.model.{ Sku, SkuDifferences }
import com.chrisluttazi.skus.spark.Spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ Matchers, WordSpec }

class SuggestionsEngineSpec extends WordSpec with Matchers with ScalaFutures {

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

      System.out.println(s"${ds.printSchema}")

      //the attribute last letter is the weight
      //a should be highest priority, while j the lowest
      //get the last letter of the att and then calculate base on a function

      //aggregate the order
      val rdd: RDD[Sku] =
        ds.rdd.map(row =>
          Sku(row(1).toString, SuggestionsEngine.convertToArray(row(0).toString)))
      //calculate the difference
      rdd
      //grab 10 best articles

    }
  }
}
