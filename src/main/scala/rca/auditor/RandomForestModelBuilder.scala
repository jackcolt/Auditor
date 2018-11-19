package rca.auditor

import org.apache.kudu.spark.kudu._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark


object RandomForestModelBuilder {

  def main(args: Array[String]): Unit = {
    try {
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

      val spark = SparkSession.builder()
        //.master("local")
        .appName("Linkage Metrics").getOrCreate()
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
          "from analysis limit 4000000")

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
      val Array(training, test) = labeledData.randomSplit(Array(0.8, 0.2), seed = 11L)
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
      model.save(sc, "/var/efsVolume/models/myRandomForestClassificationModel102")
      //val linkageModel = RandomForestModel.load(sc, "/Users/johnpoulin/tmp/myRandomForestClassificationModel")
    }

    catch {
      case e: Throwable =>
        val msg = s"Error running job:\n ${e.getMessage}"
        println(msg)
        throw e
    }
  }
}
