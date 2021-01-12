package net.sansa_stack.ml.spark.evaluation.utils

import org.apache.jena.graph.{Node, Triple}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * This class creates from a dataset of features for Similarity Models
 */
class FeatureExtractorEval extends Transformer{
  val spark = SparkSession.builder.getOrCreate()
  private val _availableModes = Array("Res")
  private var _mode: String = "Res"
  private var _outputCol: String = "extractedFeatures"

  /**
   * This method changes the features to be extracted
   * @param mode a string specifying the mode. Modes are abbreviations of their corresponding models
   * @return returns a dataframe with two columns one for the string of a respective URI and one for the feature vector
   */
  def setMode(mode: String): this.type = {
    if (_availableModes.contains(mode)) {
      _mode = mode
      this
    }
    else {
      throw new Exception("The specified mode: " + mode + "is not supported. Currently available are: " + _availableModes)
    }
  }

  def transform(dataset: Dataset[_]): DataFrame = {
    import spark.implicits._

    val ds: Dataset[(String, String, String)] = dataset.as[(String, String, String)]
    val unfoldedFeatures: Dataset[(String, String)] = _mode match {
      case "Res" => ds.flatMap(t => Seq(
        (t._1, t._3),
        (t._3, t._1)))
      case _ => throw new Exception(
        "This mode is currently not supported .\n " +
          "You selected mode " + _mode + " .\n " +
          "Currently available modes are: " + _availableModes)
    }

    val tmpDs = unfoldedFeatures
      .filter(!_._1.contains("\""))
      .groupBy("_1")
      .agg(collect_list("_2"))

    tmpDs
      .withColumnRenamed("_1", "uri")
      .withColumnRenamed("collect_list(_2)", _outputCol)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  override val uid: String = "FIXME"
}
