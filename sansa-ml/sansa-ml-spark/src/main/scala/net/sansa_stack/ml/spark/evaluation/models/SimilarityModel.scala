package net.sansa_stack.ml.spark.evaluation.models

import org.apache.spark.sql.{DataFrame, SparkSession}

trait SimilarityModel {
  val spark = SparkSession.builder.getOrCreate()
  import spark.implicits._
  private val _availableModes = Array("")
  private var _mode: String = ""

  private var _inputCols: Array[String] = Array("uri", "features")
  private var _outputCol: String = "similarityValue"

  private var t_net: Double = 0.0

  private var _target: DataFrame = spark.emptyDataFrame
  private var _features: DataFrame = spark.emptyDataFrame

  val estimatorName: String = "SimilarityEstimator"
  val estimatorMeasureType: String = "similarity"
}
