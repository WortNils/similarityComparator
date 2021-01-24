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


/**
 * This class generates similarity measures for a given array of uri pairs.
 */
class GenericSimilarityModel extends Transformer {

  // values that have to be overwritten
  protected var _similarityEstimationColumnName = "estimation"

  val estimatorName: String = "GenericSimilarityEstimator"
  val estimatorMeasureType: String = "path based, information content based, feature based or hybrid"
  val modelType: String = "SimilarityEstimator"

  override def transform(dataset: Dataset[_], target: Array[(String, String)]): DataFrame = {

  }



  // TODO: fix unimplemented functions
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  override val uid: String = "Similarity"
}
