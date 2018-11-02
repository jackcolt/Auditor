name := "Auditor"

version := "0.1"

scalaVersion := "2.11.8"

//val sparkVer = "2.2.1"
val sparkVer = "2.3.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.5",
  "org.scalatest" % "scalatest_2.11" % "3.0.5" % "test")

libraryDependencies +=   "org.apache.spark" %% "spark-core"  % sparkVer

libraryDependencies +=   "org.apache.spark" %% "spark-mllib" % sparkVer

libraryDependencies +=   "org.apache.spark" %% "spark-sql" % sparkVer

libraryDependencies +=   "org.elasticsearch" %% "elasticsearch-spark-20" % "6.4.2"

libraryDependencies += "com.sksamuel.elastic4s" %% "elastic4s-core" % "6.3.3"
libraryDependencies += "com.sksamuel.elastic4s" %% "elastic4s-http" % "6.3.3"

libraryDependencies += "com.twitter" %% "util-collection" % "18.10.0"

libraryDependencies += "org.apache.kudu" % "kudu-client" % "1.7.1"
libraryDependencies += "org.apache.kudu" %% "kudu-spark2" % "1.7.1"


