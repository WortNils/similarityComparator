package net.sansa_stack.ml.spark.evaluation.models

import scala.collection.mutable.ArrayBuffer
import org.apache.jena.graph.{Node, Triple}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.Map


/**
 * This class generates similarity measures for a given array of uri pairs.
 */
class GenericSimilarityModel extends Transformer {
  val spark = SparkSession.builder.getOrCreate()
  import spark.implicits._
  private val _availableModes = Array("res")
  private var _mode: String = "res"
  private var _depth: Int = 1
  private var _outputCol: String = "extractedFeatures"

  private var t_net: Double = 0.0

  private var _target: DataFrame = spark.emptyDataFrame
  private var _parents: DataFrame = spark.emptyDataFrame

  private var _info: Map[String, Double] = Map(" " -> 0)

  // values that have to be overwritten
  protected var _similarityEstimationColumnName = "estimation"

  val estimatorName: String = "GenericSimilarityEstimator"
  val estimatorMeasureType: String = "path based, information content based, feature based or hybrid"
  val modelType: String = "SimilarityEstimator"

  override def transform(dataset: Dataset[_]): DataFrame = {
    return dataset.toDF
  }
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  override val uid: String = "Similarity"
}
