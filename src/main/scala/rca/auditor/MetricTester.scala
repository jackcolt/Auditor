package rca.auditor

import org.apache.kudu.spark.kudu._
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.elasticsearch.spark.rdd.EsSpark
import org.scalatest.FunSuite

class MetricTester extends FunSuite {

  test("schema parser") {

    val metric = new LinkageMetric("deed linkage test", "1")


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


    var metric = new LinkageMetric("CL Deed to DQ Deed Linkage", "1")

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

    var metric = new LinkageMetric("CL Deed to DQ Deed Linkage", "1")

    println ("metrics calculated on: "+metric.timeStamp+ " in time zone: "+metric.timeZone)

    val label = new LinkageCharacteristic("apn_lev_ratio_score",0)

    metric.characteristics = label :: metric.characteristics
    metric.characteristics = new LinkageCharacteristic("addr_lev_ratio_score",0) :: metric.characteristics
    metric.characteristics = new LinkageCharacteristic("buyer_lev_ratio_score",0) :: metric.characteristics
    metric.characteristics = new LinkageCharacteristic("seller_lev_ratio_score",0) :: metric.characteristics

    metric.label = label

    metric.methodology = "LogisticRegressionWithLBFGS"

    val spark = SparkSession.builder().master("local").appName("Linkage Metrics").getOrCreate()

    spark.conf.set("spark.driver.allowMultipleContexts", "true")

    val sc = spark.sparkContext


    val sqlContext = spark.sqlContext

    val kuduMaster = "172.31.37.251:7051"

    val kuduContext = new KuduContext(kuduMaster, sc)


    val analysisDF = sqlContext.read.options(Map("kudu.master" -> "172.31.37.251:7051", "kudu.table" -> "impala::audit.dq_cl_analysis_test")).kudu

    analysisDF.createOrReplaceTempView("analysis")


    val data = spark.sql(
      "SELECT floor(1.0 - apn_lev_ratio_score) as label, "+
        "addr_lev_ratio_score, "+
        "buyer_lev_ratio_score, "+
        "seller_lev_ratio_score " +
        "date_interval_match, " +
        "date_exact_match " +
        "from analysis limit 1000")



    val labeledData = data.rdd.map(row =>
      new LabeledPoint (
        row.getAs[Long](0),
        Vectors.dense(
          row.getAs[Double](1),
          row.getAs[Double](2),
          row.getAs[Double](3),
          row.getAs[Int](4),
          row.getAs[Int](5))
      )).cache()


    // Split data into training (60%) and test (40%)
    val Array(training, test) = labeledData.randomSplit(Array(0.6, 0.4), seed = 11L)
    training.cache()

    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(training)

    // Clear the prediction threshold so the model will return probabilities
    //model.clearThreshold

    // Compute raw scores on the test set
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }
    // Instantiate metrics object
    val metrics = new MulticlassMetrics(predictionAndLabels)

    // Confusion matrix
    println("Confusion matrix:")
    println(metrics.confusionMatrix)

    // Overall Statistics
    val accuracy = metrics.accuracy
    println("Summary Statistics")
    println(s"Accuracy = $accuracy")

    // Precision by label
    val labels = metrics.labels
    labels.foreach { l =>
      println(s"Precision($l) = " + metrics.precision(l))
    }

    // Recall by label
    labels.foreach { l =>
      println(s"Recall($l) = " + metrics.recall(l))
    }

    // False positive rate by label
    labels.foreach { l =>
      println(s"FPR($l) = " + metrics.falsePositiveRate(l))
    }

    // F-measure by label
    labels.foreach { l =>
      println(s"F1-Score($l) = " + metrics.fMeasure(l))
    }

    // Weighted stats
    println(s"Weighted precision: ${metrics.weightedPrecision}")
    println(s"Weighted recall: ${metrics.weightedRecall}")
    println(s"Weighted F1 score: ${metrics.weightedFMeasure}")
    println(s"Weighted false positive rate: ${metrics.weightedFalsePositiveRate}")

    metric.performance = new Performance(metrics.accuracy, metrics.weightedPrecision,metrics.weightedRecall,metrics.weightedFMeasure,metrics.weightedFalsePositiveRate)

    val rdd = spark.sparkContext.makeRDD(Seq(metric))

    EsSpark.saveToEs(rdd, "linkage_metrics/docs")
  }

  test ("RandomForest") {

    var metric = new LinkageMetric("CL Deed to DQ Deed Linkage", "1")

    println ("metrics calculated on: "+metric.timeStamp+ " in time zone: "+metric.timeZone)

    val label = new LinkageCharacteristic("apn_lev_ratio_score",0)

    metric.characteristics = label :: metric.characteristics
    metric.characteristics = new LinkageCharacteristic("addr_lev_ratio_score",0) :: metric.characteristics
    metric.characteristics = new LinkageCharacteristic("buyer_lev_ratio_score",0) :: metric.characteristics
    metric.characteristics = new LinkageCharacteristic("seller_lev_ratio_score",0) :: metric.characteristics
    metric.characteristics = new LinkageCharacteristic("date_interval_match",1) :: metric.characteristics
    metric.characteristics = new LinkageCharacteristic("date_exact_match",1) :: metric.characteristics

    metric.label = label

    metric.methodology = "RandomForest"

    val spark = SparkSession.builder().master("local").appName("Linkage Metrics").getOrCreate()

    spark.conf.set("spark.driver.allowMultipleContexts", "true")

    val sc = spark.sparkContext


    val sqlContext = spark.sqlContext

    val kuduMaster = "172.31.37.251:7051"

    val kuduContext = new KuduContext(kuduMaster, sc)


    val analysisDF = sqlContext.read.options(Map("kudu.master" -> "172.31.37.251:7051", "kudu.table" -> "impala::audit.dq_cl_analysis_test_date")).kudu

    analysisDF.createOrReplaceTempView("analysis")


    val data = spark.sql(
      "SELECT floor(1.0 - apn_lev_ratio_score) as label, "+
        "addr_lev_ratio_score, "+
        "buyer_lev_ratio_score, "+
        "seller_lev_ratio_score, " +
        "cast (date_interval_match as DOUBLE), " +
        "cast (date_exact_match as DOUBLE)" +
        "from analysis limit 1000000")

    val labeledData = data.rdd.map(row =>
      new LabeledPoint (
        row.getAs[Long](0),
        Vectors.dense(
          row.getAs[Double](1),
          row.getAs[Double](2),
          row.getAs[Double](3),
          row.getAs[Double](4),
          row.getAs[Double](5))
      )).cache()

    // Split data into training (60%) and test (40%)
    val Array(training, test) = labeledData.randomSplit(Array(0.6, 0.4), seed = 11L)
    training.cache()

    val numClasses = 2
    val numTrees = 256 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32
    val categoricalFeaturesInfo = Map[Int, Int]()

    val model = RandomForest.trainClassifier(training, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = test.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / test.count()
    println(s"Test Error = $testErr")
    println(s"Learned classification forest model:\n ${model.toDebugString}")


    // Instantiate metrics object
    val metrics = new MulticlassMetrics(labelAndPreds)

    // Confusion matrix
    println("Confusion matrix:")
    println(metrics.confusionMatrix)

    // Overall Statistics
    val accuracy = metrics.accuracy
    println("Summary Statistics")
    println(s"Accuracy = $accuracy")

    // Precision by label
    val labels = metrics.labels
    labels.foreach { l =>
      println(s"Precision($l) = " + metrics.precision(l))
    }

    // Recall by label
    labels.foreach { l =>
      println(s"Recall($l) = " + metrics.recall(l))
    }

    // False positive rate by label
    labels.foreach { l =>
      println(s"FPR($l) = " + metrics.falsePositiveRate(l))
    }

    // F-measure by label
    labels.foreach { l =>
      println(s"F1-Score($l) = " + metrics.fMeasure(l))
    }

    // Weighted stats
    println(s"Weighted precision: ${metrics.weightedPrecision}")
    println(s"Weighted recall: ${metrics.weightedRecall}")
    println(s"Weighted F1 score: ${metrics.weightedFMeasure}")
    println(s"Weighted false positive rate: ${metrics.weightedFalsePositiveRate}")


    metric.performance = new Performance(metrics.accuracy, metrics.weightedPrecision,metrics.weightedRecall,metrics.weightedFMeasure,metrics.weightedFalsePositiveRate)

    val rdd = spark.sparkContext.makeRDD(Seq(metric))

    EsSpark.saveToEs(rdd, "model_metrics/docs")


    //val stream = this.getClass.getResource("model.myRandomForestClassificationModel100").toString


    // Save and load model
    model.save(sc, "/Users/johnpoulin/tmp/myRandomForestClassificationModel102")
    //val linkageModel = RandomForestModel.load(sc, "/Users/johnpoulin/tmp/myRandomForestClassificationModel")
  }
}

