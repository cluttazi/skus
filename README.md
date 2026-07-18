# skus

[![CI](https://github.com/cluttazi/skus/actions/workflows/ci.yml/badge.svg)](https://github.com/cluttazi/skus/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Scala](https://img.shields.io/badge/Scala-2.13-red.svg)](https://www.scala-lang.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-4.1-e25a1c.svg)](https://spark.apache.org/)

A product-similarity engine built on Apache Spark: given a catalogue of SKUs described by attribute vectors, it recommends the most similar products for any given SKU.

## How It Works

Each SKU is described by a set of attributes (`att-a` … `att-j`) with integer values. The `SuggestionsEngine`:

1. **Parses** a JSON dataset into an `RDD[Sku]`
2. **Compares** SKUs pairwise by computing the element-wise absolute difference of their attribute vectors
3. **Ranks** candidates by total difference, using a positional *breaker* (a base-100 weighted sum that favours differences in earlier attributes) to break ties deterministically
4. **Returns** the top-N most similar SKUs as recommendations

Example: `sku-1 [1,1,1]` is more similar to `sku-2 [2,1,1]` than to `sku-3 [1,3,3]`.

## Tech Stack

- Scala 2.13.18
- Apache Spark 4.1.3 (Core, SQL)
- ScalaTest 3.2
- sbt 1.11 (with scalafmt for formatting)

## Getting Started

### Prerequisites

- JDK 21 (any 17+ works; the build passes the required `--add-opens` flags to the forked test JVM)
- [sbt](https://www.scala-sbt.org/) (any recent version; the correct sbt version is picked up from `project/build.properties`)

### Run the Test Suite

```bash
sbt clean test
```

The suite runs the full recommendation pipeline against the sample dataset in `src/test/resources/input.json` using a local Spark session, and logs the generated suggestions — no external infrastructure required.

### Input Format

The engine consumes newline-delimited JSON records:

```json
{"sku": "sku-1", "attributes": "att-a-3,att-b-0,att-c-8,att-d-7,att-e-7,att-f-4,att-g-3,att-h-2,att-i-13,att-j-1"}
```

## Project Structure

```
src
├── main/scala/com/chrisluttazi/skus/
│   ├── SuggestionsEngine.scala        # Core comparison + recommendation logic
│   ├── model/
│   │   ├── Sku.scala                  # Sku and SkuDifferences case classes
│   │   └── SuggestionsEngineT.scala   # Engine contract
│   └── spark/Spark.scala              # Local SparkSession provider
└── test/
    ├── scala/.../SuggestionsEngineSpec.scala
    └── resources/input.json           # Sample SKU dataset
```

## Continuous Integration

Every push and pull request runs `sbt scalafmtCheckAll scalafmtSbtCheck` and `sbt test` on Temurin JDK 21 via [GitHub Actions](.github/workflows/ci.yml).

## License

This project is licensed under the MIT License – see [LICENSE](LICENSE) for details.
