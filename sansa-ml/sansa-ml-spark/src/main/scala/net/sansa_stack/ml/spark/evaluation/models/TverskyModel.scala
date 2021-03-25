package net.sansa_stack.ml.spark.evaluation.models

import net.sansa_stack.ml.spark.evaluation.utils.FeatureExtractorEval
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import scala.collection.mutable.WrappedArray.ofRef

import scala.collection.Map

/**
 * This class takes a base dataset and a target DataFrame and returns the Resnik similarity value
 * and the time it took to arrive at that value in a DataFrame
 */
class TverskyModel extends Transformer {
  val spark = SparkSession.builder.getOrCreate()
  import spark.implicits._
  private val _availableModes = Array("tver")
  private var _mode: String = "tver"
  private var _depth: Int = 1
  private var _outputCol: String = "extractedFeatures"

  private var t_net: Double = 0.0

  private var _target: DataFrame = spark.emptyDataFrame
  private var _parents: DataFrame = spark.emptyDataFrame

  private var _info: Map[String, Double] = Map(" " -> 0)

  val estimatorName: String = "TverskySimilarityEstimator"
  val estimatorMeasureType: String = "similarity"

  /**
   * This function takes a list of features for two entities a and b and returns their Tversky similarity value
   * and the time it took to calculate that value
   * @param a List of parents for entity a
   * @param b List of parents for entity b
   * @return 2-Tuple of Tversky value and time taken
   */
  def tverskyMethod(a: List[String], b: List[String]): Tuple2[Double, Double] = {
    (0.0, 0.0)
  }

  /**
   * udf to invoke the ResnikMethod with the correct parameters
   */
  protected val tversky = udf((a: ofRef[String], b: ofRef[String]) => {
    /* a.keySet.intersect(b.keySet).map(k => k->(a(k),b(k))). */
    tverskyMethod(a.toList, b.toList)
  })

  /**
   * This method sets the iteration depth for the parent feature extraction
   * @param depth Integer value indicating how deep parents are searched for
   * @return the Resnik model
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
   * This method sets the target Dataframe for this Model
   * @param target target Dataframe with pairs of entities
   * @return the Resnik model
   */
  def setTarget(target: DataFrame): this.type = {
    _target = target
    this
  }

  /**
   * Takes read in dataframe, and target dataframe and produces a dataframe with similarity values
   * @param dataset a dataframe read in over sansa rdf layer
   * @return a dataframe with four columns, two for the entities, one for the similarity value and one for the time
   */
  def transform (dataset: Dataset[_]): DataFrame = {
    // timekeeping
    val t0 = System.nanoTime()

    // parent calculation
    val featureExtractorModel = new FeatureExtractorEval()
      .setMode("feat").setDepth(_depth)
      .setTarget(_target.drop("entityA")
        .withColumnRenamed("entityB", "uri")
        .union(_target.drop("entityB")
          .withColumnRenamed("entityA", "uri"))
        .distinct())
    val features = featureExtractorModel
      .transform(dataset)

    val target = _target.join(features, _target("entityA") === features("entity"))
      .withColumnRenamed("features", "featuresA")
      .join(features, _target("entityB") === features("entity"))
      .withColumnRenamed("features", "featuresB")

    // timekeeping
    val t1 = System.nanoTime()
    t_net = t1 - t0

    val result = target.withColumn("TverskyTemp", tversky(col("featuresA"), col("featuresB")))
      .drop("featuresA", "featuresB")
    result.withColumn("Tversky", result("TverskyTemp._1"))
      .withColumn("TverskyTime", result("TverskyTemp._2"))
      .drop("TverskyTemp")
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  override val uid: String = "FIXME"
}
