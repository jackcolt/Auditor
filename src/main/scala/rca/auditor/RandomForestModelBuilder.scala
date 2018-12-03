package rca.auditor

import org.apache.kudu.spark.kudu._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark
import org.joda.time.DateTime


object RandomForestModelBuilder {


  val esNodes = "dm-test-es.rcanalytics.io:9200"

  def main(args: Array[String]): Unit = {
    try {
      var metric = new LinkageMetric("CL Deed to DQ Deed Linkage Model", "1")

      val label = new LinkageCharacteristic("apn_lev_ratio_score",0)

      metric.characteristics = label :: metric.characteristics
      metric.characteristics = new LinkageCharacteristic("addr_lev_ratio_score",0) :: metric.characteristics
      metric.characteristics = new LinkageCharacteristic("buyer_lev_ratio_score",0) :: metric.characteristics
      metric.characteristics = new LinkageCharacteristic("seller_lev_ratio_score",0) :: metric.characteristics
      metric.characteristics = new LinkageCharacteristic("date_interval_match",1) :: metric.characteristics
      metric.characteristics = new LinkageCharacteristic("date_exact_match",1) :: metric.characteristics

      metric.label = label

      metric.source = new LinkageSource("DQ");
      metric.target = new LinkageSource("CL");

      metric.methodology = "RandomForest"

      val spark = SparkSession.builder()
        //.master("local")
      .appName("Random Forest Model").getOrCreate()

      spark.conf.set("spark.driver.allowMultipleContexts", "true")

      //refactor to take a spark context as a parameter from a driver class.

      //spark.conf.set("es.nodes", "172.31.35.124")

      //spark.conf.set("es.port", "9200")

      spark.conf.set("es.index.auto.create", "true")

      //spark.conf.set("es.nodes.discovery", "false")

      //spark.conf.set("es.nodes.wan.only", "false")

      val sc = spark.sparkContext

      logMessage(metric,"metrics calculated on: "+metric.timeStamp+ " in time zone: "+metric.timeZone, spark)

      val sqlContext = spark.sqlContext

      val kuduMaster = "172.31.37.251:7051"

      val kuduContext = new KuduContext(kuduMaster, sc)


      val analysisDF = sqlContext.read.options(Map("kudu.master" -> "172.31.37.251:7051", "kudu.table" -> "impala::audit.dq_cl_analysis_test_date")).kudu

      analysisDF.createOrReplaceTempView("analysis")


      val data = spark.sql(

        //"SELECT floor(1.0 - apn_lev_ratio_score) as label, "+
          "select (1.0 - apn_lev_ratio_score), "+
          "addr_lev_ratio_score, "+
          "buyer_lev_ratio_score, "+
          "seller_lev_ratio_score, " +
          "cast (date_interval_match as DOUBLE), " +
          "cast (date_exact_match as DOUBLE) " +
          "from analysis " +
            "where apn_lev_ratio_score=1 or apn_lev_ratio_score=0")

      val labeledData = data.rdd.map(row =>
        new LabeledPoint (
          row.getAs[Double](0),
          Vectors.dense(
            row.getAs[Double](1),
            row.getAs[Double](2),
            row.getAs[Double](3),
            row.getAs[Double](4),
            row.getAs[Double](5))
        )).cache()

      //use case classes for serialization instead of BeanProperty
      //define a case class for the sql query and use type safe item name for the vectors. (data set)
      //convert null charactertistics to "1"
      //convert to new table structure that is more generic
      //leverage linkage characteristics defined in model meta-data use string interpolation

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

      logMessage(metric,s"Test Error = $testErr", spark)
      logMessage(metric,s"Learned classification forest model:\n ${model.toDebugString}", spark)



      // Instantiate metrics object
      val metrics = new MulticlassMetrics(labelAndPreds)

      // Confusion matrix

      logMessage(metric,"Confusion matrix:", spark)
      logMessage(metric,metrics.confusionMatrix.toString(), spark)

      // Overall Statistics
      val accuracy = metrics.accuracy

      logMessage(metric,s"Accuracy = $accuracy", spark)

      // Precision by label
      val labels = metrics.labels

      val values= StringBuilder.newBuilder
      labels.foreach { l =>
        val msg = s"Precision($l) = " + metrics.precision(l) + " "
        logMessage(metric,msg, spark)

        values.append(msg)
      }


      // Recall by label
      labels.foreach { l =>
        logMessage(metric,s"Recall($l) = " + metrics.recall(l), spark)
      }

      // False positive rate by label
      labels.foreach { l =>
        logMessage(metric,s"FPR($l) = " + metrics.falsePositiveRate(l), spark)
      }

      // F-measure by label
      labels.foreach { l =>
        logMessage(metric,s"F1-Score($l) = " + metrics.fMeasure(l), spark)
      }

      // Weighted stats

      logMessage(metric,s"Weighted precision: ${metrics.weightedPrecision}", spark)
      logMessage(metric,s"Weighted recall: ${metrics.weightedRecall}", spark)
      logMessage(metric,s"Weighted F1 score: ${metrics.weightedFMeasure}", spark)
      logMessage(metric,s"Weighted false positive rate: ${metrics.weightedFalsePositiveRate}", spark)


      val modelLocation = "/var/efsVolume/models/rf_"+DateTime.now().toString("HH_mm_ss")

      logMessage(metric,s"storing model to: ${modelLocation}", spark)

      metric.performance = new Performance(
        metrics.accuracy,
        metrics.weightedPrecision,
        metrics.weightedRecall,
        metrics.weightedFMeasure,
        metrics.weightedFalsePositiveRate,
        values.toString(),
        metrics.confusionMatrix.toString(),
        modelLocation)


      val rdd = spark.sparkContext.makeRDD(Seq(metric))

      //this is a hack since I could not get this to work in setting the spark context.

      EsSpark.saveToEs(rdd, "linkage_models/docs", Map("es.nodes" -> "dm-test-es.rcanalytics.io:9200"))


      // Save and load model
      model.save(sc, modelLocation)
      //val linkageModel = RandomForestModel.load(sc, "/Users/johnpoulin/tmp/myRandomForestClassificationModel")

    }

    catch {
      case e: Throwable =>
        val msg = s"Error running job:\n ${e.getMessage}"
        println(msg)
        throw e
    }
  }

  def logMessage( metric: LinkageMetric, message:String, sparkSession: SparkSession): Unit = {
    val logEntry = new LogEntry(metric, message)
    val rdd = sparkSession.sparkContext.makeRDD(Seq(logEntry))
    EsSpark.saveToEs(rdd, "metrics_log/docs",Map("es.nodes" -> s"$esNodes") )
  }

}
