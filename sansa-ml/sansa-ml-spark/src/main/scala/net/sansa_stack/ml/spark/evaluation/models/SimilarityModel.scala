package net.sansa_stack.ml.spark.evaluation.models

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

trait SimilarityModel {
  // generic variables
  val spark = SparkSession.builder.getOrCreate()
  import spark.implicits._
  private var t_net: Double = 0.0

  // input dataframes
  private var _target: DataFrame = spark.emptyDataFrame
  private var _features: DataFrame = spark.emptyDataFrame

  // meta information
  val estimatorName: String = "SimilarityEstimator"
  val estimatorMeasureType: String = "path based, information content based, feature based or hybrid"
  val modelType: String = "SimilarityEstimator"

  // model specific variables
  private val _availableModes = Array("")
  private var _mode: String = ""

  private var _outputCol: String = "similarityValue"

  def transform (dataset: Dataset[_]): DataFrame
}
