

name := "Auditor"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVer = "2.2.1"
//val sparkVer = "2.3.2"

resolvers ++= Seq(
  Resolver.sonatypeRepo("public"),
  "Confluent Maven Repo" at "http://packages.confluent.io/maven/"
)


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


/* without this explicit merge strategy code you get a lot of noise from sbt-assembly
   complaining about not being able to dedup files */
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

/* including scala bloats your assembly jar unnecessarily, and may interfere with
   spark runtime */
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := "model_training.jar"
