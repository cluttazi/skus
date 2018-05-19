package com.chrisluttazi.skus.model

// Represents an SKU
case class Sku(sku: String, attributes: Array[Int])

// Represents the difference between two skus
case class SkuDifferences(sku: Sku, difference: Int, breaker: BigInt)