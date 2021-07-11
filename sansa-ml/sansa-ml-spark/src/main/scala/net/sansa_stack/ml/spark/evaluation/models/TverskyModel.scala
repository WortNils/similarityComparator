package net.sansa_stack.ml.spark.evaluation.models

import net.sansa_stack.ml.spark.evaluation.utils.FeatureExtractorEval
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType


/**
 * This class takes a base dataset and a target DataFrame and returns the Resnik similarity value
 * and the time it took to arrive at that value in a DataFrame
 */
class TverskyModel extends Transformer with SimilarityModel {
  override val spark: SparkSession = SparkSession.builder.getOrCreate()
  import spark.implicits._
  private val _availableModes = Array("tver")
  private var _mode: String = "tver"
  private var _depth: Int = 1

  private var _inputCols: Array[String] = Array("uri", "vectorizedFeatures")
  private var _outputCol: String = "extractedFeatures"

  private var t_net: Double = 0.0

  protected var _target: DataFrame = spark.emptyDataFrame
  protected var _features: DataFrame = spark.emptyDataFrame

  private var _alpha: Double = 1.0
  private var _beta: Double = 1.0

  override val estimatorName: String = "TverskySimilarityEstimator"
  override val estimatorMeasureType: String = "feature based"
  override val modelType: String = "Tversky"

  protected val tversky = udf((a: Vector, b: Vector, alpha: Double, beta: Double) => {
    // Timekeeping
    val t2 = System.currentTimeMillis()

    val featureIndicesA = a.toSparse.indices
    val featureIndicesB = b.toSparse.indices
    val fSetA = featureIndicesA.toSet
    val fSetB = featureIndicesB.toSet
    if (fSetA.union(fSetB).isEmpty) {
      // Timekeeping
      val t3 = System.currentTimeMillis()
      val t_diff = (t_net + t3 - t2)/1000

      (0.0, t_diff)
    }
    else {
      val tversky: Double = fSetA.intersect(fSetB).size.toDouble/(
            fSetA.intersect(fSetB).size.toDouble
              +  (alpha * fSetA.diff(fSetB).size.toDouble)
              + (beta * fSetB.diff(fSetA).size.toDouble)
            )


      // Timekeeping
      val t3 = System.currentTimeMillis()
      val t_diff = (t_net + t3 - t2)/1000
      (tversky, t_diff)
    }
  })

  def setAlpha(a: Double): this.type = {
    if (a < 0 || a > 1) {
      throw new Error("PROBLEM: alpha has to be between 0 and 1")
    }
    else {
      _alpha = a
      this
    }
  }

  def setBeta(b: Double): this.type = {
    if (b < 0 || b > 1) {
      throw new Error("PROBLEM: alpha has to be between 0 and 1")
    }
    else {
      _beta = b
      this
    }
  }

/*
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
 */

  /**
   * This method sets the iteration depth for the parent feature extraction
   * @param depth Integer value indicating how deep parents are searched for
   * @return the Resnik model
   */
  def setDepth(depth: Int): this.type = {
    if (depth > 0) {
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
   * Insert features into the model with a uri column, a parent column and the information
   * content of the parent in another column
   * @param features DataFrame with the data
   * @param uri name of the uri column
   * @param vectorizedFeatures name of the features column
   * @return the Tversky model
   */
  def setFeatures(features: DataFrame, uri: String, vectorizedFeatures: String): this.type = {
    if (!features.isEmpty) {
      _features = features.select(uri, vectorizedFeatures)
        .withColumnRenamed(uri, _inputCols(0))
        .withColumnRenamed(vectorizedFeatures, _inputCols(1))
      this
    }
    else {
      throw new Exception("Features DataFrame must not be empty")
    }
  }

  /**
   * Takes read in dataframe, and target dataframe and produces a dataframe with similarity values
   * @param dataset a dataframe read in over sansa rdf layer
   * @return a dataframe with four columns, two for the entities, one for the similarity value and one for the time
   */
  def transform (dataset: Dataset[_]): DataFrame = {
    if (_features.isEmpty) {
      // timekeeping
      val t0 = System.currentTimeMillis()

      // feature calculation
      val tempDf = _target.drop("entityA")
        .withColumnRenamed("entityB", "uri")
        .union(_target.drop("entityB")
          .withColumnRenamed("entityA", "uri"))
        .distinct()
      val featureExtractorModel = new FeatureExtractorEval()
        .setMode("feat").setDepth(_depth)
        .setTarget(tempDf)
      val features = featureExtractorModel
        .transform(dataset)

      val target = _target.join(features, _target("entityA") === features("uri")).drop("uri")
        .withColumnRenamed("vectorizedFeatures", "featuresA")
        .join(features, _target("entityB") === features("uri")).drop("uri")
        .withColumnRenamed("vectorizedFeatures", "featuresB")

      // TODO: insert Feature compatibility

      // timekeeping
      val t1 = System.currentTimeMillis()
      t_net = t1 - t0

      target.count()

      val result = target.withColumn("TverskyTemp", tversky(col("featuresA"), col("featuresB"), lit(_alpha), lit(_beta)))
        .drop("featuresA", "featuresB")
      result.withColumn("Tversky", result("TverskyTemp._1"))
        .withColumn("TverskyTime", result("TverskyTemp._2"))
        .drop("TverskyTemp")
    }
    else {
      // timekeeping
      val t0 = System.currentTimeMillis()

      val features = _features.select(_inputCols(0), _inputCols(1))

      val target = _target.join(features, _target("entityA") === features("uri")).drop("uri")
        .withColumnRenamed("vectorizedFeatures", "featuresA")
        .join(features, _target("entityB") === features("uri")).drop("uri")
        .withColumnRenamed("vectorizedFeatures", "featuresB")

      // TODO: insert Feature compatibility

      // timekeeping
      val t1 = System.currentTimeMillis()
      t_net = t1 - t0

      target.count()

      val result = target.withColumn("TverskyTemp", tversky(col("featuresA"), col("featuresB"), lit(_alpha), lit(_beta)))
        .drop("featuresA", "featuresB")
      result.withColumn("Tversky", result("TverskyTemp._1"))
        .withColumn("TverskyTime", result("TverskyTemp._2"))
        .drop("TverskyTemp")
    }
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  override val uid: String = "FIXME"
}
