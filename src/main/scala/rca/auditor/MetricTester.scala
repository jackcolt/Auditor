package rca.auditor

import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.elasticsearch.spark.rdd.EsSpark
import org.scalatest.FunSuite

class MetricTester extends FunSuite {

  test("schema parser") {

    val metric = new LinkageMetric("deed linkage test")


    val linkageSource = new LinkageSource()
    linkageSource.name = "CL deeds"
    linkageSource.eligiblePopulation = 10
    linkageSource.population = 100
    metric.source = linkageSource

    val linkageTarget = new LinkageSource()
    linkageTarget.name = "DQ deeds"
    linkageTarget.eligiblePopulation = 10
    linkageTarget.population = 100
    metric.target = linkageTarget

    val characteristic = new LinkageCharacteristic("APN")


    metric.characteristics = characteristic :: metric.characteristics

    println(metric.name)

    println(metric.source.name)

    for (c <- metric.characteristics) println(c.name)

    val spark = SparkSession.builder().master("local").appName("Linkage Metrics").getOrCreate()
    spark.conf.set("es.index.auto.create", "true")

    val rdd = spark.sparkContext.makeRDD(Seq(metric))

    EsSpark.saveToEs(rdd, "metric_test/docs")

  }

  test("from database read") {

    //val spark = SparkSession.builder().master("spark://172.31.30.117:7077").appName("Linkage Metrics").getOrCreate()

    val spark = SparkSession.builder().master("local").appName("Linkage Metrics").getOrCreate()
    //val conf = new SparkConf().setAppName("Linkage_DQ_VS_CL").setMaster("spark://172.31.30.117:7077")

    spark.conf.set("spark.driver.allowMultipleContexts", "true")

    val sc = spark.sparkContext


    val sqlContext = spark.sqlContext

    val kuduMaster = "172.31.37.251:7051"

    val kuduContext = new KuduContext(kuduMaster, sc)

    //val masterAssessorDF = sqlContext.read.options(Map("kudu.master" -> "172.31.37.251:7051", "kudu.table" -> "audit.master_assessor")).kudu
    //val masterAssessorGeoIdDF = sqlContext.read.options(Map("kudu.master" -> "172.31.37.251:7051", "kudu.table" -> "dev.master_assessor_geo_id")).kudu
    //val masterTransactionsDF = sqlContext.read.options(Map("kudu.master" -> "172.31.37.251:7051", "kudu.table" -> "audit.master_transactions")).kudu
    //val deedDF = sqlContext.read.options(Map("kudu.master" -> "172.31.37.251:7051", "kudu.table" -> "test.corelogic_deed")).kudu
    //val parcelDF = sqlContext.read.options(Map("kudu.master" -> "172.31.37.251:7051", "kudu.table" -> "test.corelogic_parcel")).kudu

    val analysisDF = sqlContext.read.options(Map("kudu.master" -> "172.31.37.251:7051", "kudu.table" -> "impala::audit.dq_cl_analysis_test")).kudu


    //val head = analysisDF.take(20)
    //head.foreach(println)

    analysisDF.createGlobalTempView("analysis")

    var metric = new LinkageMetric("CL Deed to DQ Deed Linkage")

    metric.characteristics = new LinkageCharacteristic("apn_lev_ratio_score") :: metric.characteristics
    //metric.characteristics = new LinkageCharacteristic("addr_lev_ratio_score") :: metric.characteristics
    //metric.characteristics = new LinkageCharacteristic("buyer_lev_ratio_score") :: metric.characteristics
    //metric.characteristics = new LinkageCharacteristic("seller_lev_ratio_score") :: metric.characteristics

    analysisDF.cache()

    for (m <- metric.characteristics) {

      val facts = analysisDF.filter("0<>"+m.name).agg(
        count(m.name),
        avg(m.name),
        min(m.name),
        max(m.name),
        mean(m.name),
        stddev(m.name))

      for (r <- facts.take(1)) {
        m.sample = r.getAs[Int](0)
        m.avg = r.getAs[Float](1)
        m.min = r.getAs[Float](2)
        m.max = r.getAs[Float](3)
        m.mean = r.getAs[Float](4)
        m.stddev = r.getAs[Float](5)
      }
    }

    for (m <- metric.characteristics) {
      println(m.name)
      print("count:" + m.sample)
      println ("avg: " + m.avg)
    }
  }

}

