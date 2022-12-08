import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.11"
  lazy val sparkCore  = "org.apache.spark" %% "spark-core" % "3.2.1"
  lazy val sparkSQL  = "org.apache.spark" %% "spark-sql" % "3.2.1"
  lazy val sparkStreaming  = "org.apache.spark" %% "spark-streaming" % "3.2.1"
  lazy val deltaLake  = "io.delta" %% "delta-core" % "2.0.0"
}
