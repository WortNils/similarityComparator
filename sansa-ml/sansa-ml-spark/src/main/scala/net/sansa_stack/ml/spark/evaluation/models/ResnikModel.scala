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
import org.apache.spark.sql.functions._
import net.sansa_stack.ml.spark.evaluation.utils.FeatureExtractorEval

class ResnikModel extends GenericSimilarityModel {

  override def transform(dataset: Dataset[_], target: DataFrame): DataFrame = {
    val featureExtractorModel = new FeatureExtractorEval()
      .setMode("par")
    val parents = featureExtractorModel
      .transform(dataset, target)

    map((row: (String, String, _)) =>
      val a = parents.where("UriA" == "UriA").drop(col1, col2)
      val b = parents.where("UriB" == "UriB").drop(col1, col2)
      val common: DataFrame = a.intersect(b)

      featureExtractorModel.setMode("ic")
      val informationContent = featureExtractorModel
      .transform(dataset, target)
      val resnik = informationContent.sort(desc(columnName = informationContent)).first()
    )
  }

  override val estimatorName: String = "ResnikSimilarityEstimator"
  override val estimatorMeasureType: String = "similarity"

  override val similarityEstimation = resnik
}
