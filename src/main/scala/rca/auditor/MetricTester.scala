package rca.auditor

import org.apache.kudu.spark.kudu._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.elasticsearch.spark.rdd.EsSpark
import org.scalatest.FunSuite

class MetricTester extends FunSuite {

  test("schema parser") {

    val metric = new LinkageMetric("deed linkage test")


    val linkageSource = new LinkageSource("CL Deeds")

    linkageSource.eligiblePopulation = 10
    linkageSource.population = 100
    metric.source = linkageSource

    val linkageTarget = new LinkageSource("DQ deeds")

    linkageTarget.eligiblePopulation = 10
    linkageTarget.population = 100
    metric.target = linkageTarget

    val characteristic = new LinkageCharacteristic("APN",0)


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


    val analysisDF = sqlContext.read.options(Map("kudu.master" -> "172.31.37.251:7051", "kudu.table" -> "impala::audit.dq_cl_analysis_test")).kudu


    //val head = analysisDF.take(20)
    //head.foreach(println)


    var metric = new LinkageMetric("CL Deed to DQ Deed Linkage")

    println ("metrics calculated on: "+metric.timeStamp+ " in time zone: "+metric.timeZone)

    metric.characteristics = new LinkageCharacteristic("apn_lev_ratio_score",0) :: metric.characteristics
    metric.characteristics = new LinkageCharacteristic("addr_lev_ratio_score",0) :: metric.characteristics
    metric.characteristics = new LinkageCharacteristic("buyer_lev_ratio_score",0) :: metric.characteristics
    metric.characteristics = new LinkageCharacteristic("seller_lev_ratio_score",0) :: metric.characteristics




    for (m <- metric.characteristics) {


      val facts = analysisDF.agg(
        count(m.name),
        avg(m.name),
        min(m.name),
        max(m.name),
        mean(m.name),
        stddev(m.name))


      for (r <- facts.take(1)) {
        m.sample = r.getAs[Long](0)
        m.avg = r.getAs[Double](1)
        m.min = r.getAs[Double](2)
        m.max = r.getAs[Double](3)
        m.mean = r.getAs[Double](4)
        m.stddev = r.getAs[Double](5)
      }
      m.hits=analysisDF.filter(m.name+"="+m.hitScore).count()
    }
    val rdd = spark.sparkContext.makeRDD(Seq(metric))

    EsSpark.saveToEs(rdd, "linkage_metrics/docs")


  }

  test ("Regression Test") {

    //val spark = SparkSession.builder().master("spark://172.31.30.117:7077").appName("Linkage Metrics").getOrCreate()

    val spark = SparkSession.builder().master("local").appName("Linkage Metrics").getOrCreate()
    //val conf = new SparkConf().setAppName("Linkage_DQ_VS_CL").setMaster("spark://172.31.30.117:7077")

    spark.conf.set("spark.driver.allowMultipleContexts", "true")

    val sc = spark.sparkContext


    val sqlContext = spark.sqlContext

    val kuduMaster = "172.31.37.251:7051"

    val kuduContext = new KuduContext(kuduMaster, sc)


    val analysisDF = sqlContext.read.options(Map("kudu.master" -> "172.31.37.251:7051", "kudu.table" -> "impala::audit.dq_cl_analysis_test")).kudu

    analysisDF.createOrReplaceTempView("analysis")


    val data = spark.sql(
      "SELECT (1.0 - apn_lev_ratio_score) as label, addr_lev_ratio_score, buyer_lev_ratio_score from analysis " +
    "limit 1000")



    val trainingData = data.rdd.map(row =>
      new LabeledPoint (
        BigDecimal(row.getAs[Double](0)).setScale(0, BigDecimal.RoundingMode.FLOOR).toDouble,
        Vectors.dense(row.getAs[Double](row.length-1))
      )).cache()

    // Building the model
    val numIterations = 100
    val stepSize = 0.00000001


    val model = LinearRegressionWithSGD.train(trainingData, numIterations, stepSize)


    // Evaluate model on training examples and compute training error
    val valuesAndPreds = trainingData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
    println(s"training Mean Squared Error $MSE")

  }
}

