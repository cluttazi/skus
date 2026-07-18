lazy val sparkVersion = "4.1.3"

lazy val root = (project in file("."))
  .settings(
    inThisBuild(
      List(
        organization := "com.chrisluttazi",
        scalaVersion := "2.13.18"
      )
    ),
    name := "skus",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.scalatest" %% "scalatest" % "3.2.20" % Test
    ),
    // Spark on JDK 17+ needs the JVM module system opened up; fork the test
    // JVM so these options apply to the JVM actually running Spark.
    Test / fork := true,
    Test / parallelExecution := false,
    Test / javaOptions ++= Seq(
      "-XX:+IgnoreUnrecognizedVMOptions",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    )
  )
