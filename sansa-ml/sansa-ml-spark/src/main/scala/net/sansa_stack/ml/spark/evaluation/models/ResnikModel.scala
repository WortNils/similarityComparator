package net.sansa_stack.ml.spark.evaluation.models

import net.sansa_stack.ml.spark.similarity.similarityEstimationModels._
import net.sansa_stack.ml.spark.utils.{FeatureExtractorModel, SimilarityExperimentMetaGraphFactory}
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.ml.spark.similarity.similarityEstimationModels.GenericSimilarityEstimatorModel
import org.apache.jena.graph
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class ResnikModel extends GenericSimilarityEstimatorModel {

  protected val resnik = udf(a: DataFrame, b: DataFrame) => {
    val common: DataFrame = a.intersect(b)
    val resnik = common.sort("informationContent", descending).first()
  }

  override val estimatorName: String = "ResnikSimilarityEstimator"
  override val estimatorMeasureType: String = "similarity"

  override val similarityEstimation = resnik
}
