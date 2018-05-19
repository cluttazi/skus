lazy val sparkVersion = "2.3.0"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.chrisluttazi",
      scalaVersion := "2.11.8"
    )),
    name := "skus",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-hive" % sparkVersion,
      "org.scalatest" %% "scalatest" % "3.0.1" % Test
    )
  )
