package net.sansa_stack.ml.spark.evaluation.utils

import scala.collection.mutable.ArrayBuffer
import org.apache.jena.graph.{Node, Triple}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import net.sansa_stack.ml.spark.utils.FeatureExtractorModel
import org.apache.spark.sql.Row


/**
 * This class creates from a dataset of triples features for Similarity Models
 */
class FeatureExtractorEval extends Transformer{
  val spark = SparkSession.builder.getOrCreate()
  private val _availableModes = Array("par", "ic")
  private var _mode: String = "par"
  private var _depth: Int = 1
  private var _outputCol: String = "extractedFeatures"

  /**
   * This method changes the features to be extracted
   * @param mode a string specifying the mode. Modes are abbreviations of their corresponding models
   * @return returns the FeatureExtractor
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

  /**
   * This method sets the depth parameter for the parentFinder
   * @param depth an integer specifying the depth to which parents are searched. Default value 1.
   * @return returns the FeatureExtractor
   */
  def setDepth(depth: Int): this.type = {
    if (depth > 1) {
      _depth = depth
      this
    }
    else {
      throw new Exception("Depth must be at least 1.")
    }
  }

  /**
   * private method for depth-search of parent nodes
   * @param parents DataFrame of depth 0 parents
   * @param data full scope DataFrame
   * @return returns a DataFrame of all parents up to _depth
   */
  private def findParents(parents: DataFrame, data: DataFrame): DataFrame = {
    import spark.implicits._
    val search: DataFrame = parents
    for (var i <- 0 to (_depth - 1)) {
       while (!search.isEmpty) {
         search.foreach{row =>
           parents = parents.union(data.filter('_1 == row(0)))
           search = search.filter('_1 != row(0))
           // TODO: find out what is wrong with filter
         }
       }
    }
  }

  /**
   * Takes read in dataframe and produces a dataframe with features
    * @param dataset a dataframe read in over sansa rdf layer
   * @return a dataframe with two columns, one for string of URI and one of a list of features
   */
  def transform(dataset: Dataset[_], target: DataFrame): DataFrame = {
    import spark.implicits._
    val unfoldedFeatures: Dataset[(String, _)] = _mode match {
      case "par" => {
        val featureExtractorModel = new FeatureExtractorModel()
          .setMode("in")
        val extractedFeatures = featureExtractorModel
          .transform(dataset)

        // TODO: refine target column names
        val uris = target.select("col1").union(target.select("row2")).distinct()

        // uris are considered parents of depth 0
        uris.flatMap { row => Seq(row(0), findParents(row.toDF, extractedFeatures).toString) }
        // map uri to each parent, then collect list at the end
      }
      case "ic" => {
        val featureExtractorModel = new FeatureExtractorModel()
          .setMode("an")
        val extractedFeatures = featureExtractorModel
          .transform(dataset)

        // TODO: find better way to filter for uris
        val triples = extractedFeatures.count()
        target.flatMap({ row =>
          val entityTriples = extractedFeatures
            .filter(t => t.getAs[String]("uri").equals(row(0))).count()
          Seq(row(0), entityTriples / triples)
        })
      }
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
