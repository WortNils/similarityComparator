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
class ResnikModel extends Transformer {
  val spark = SparkSession.builder.getOrCreate()
  import spark.implicits._
  private val _availableModes = Array("res")
  private var _mode: String = "res"
  private var _depth: Int = 1

  private var _inputCols: Array[String] = Array("uri", "parent", "informationContent")
  private var _outputCol: String = "extractedFeatures"

  private var t_net: Double = 0.0

  private var _target: DataFrame = spark.emptyDataFrame
  private var _parents: DataFrame = spark.emptyDataFrame
  private var _features: DataFrame = spark.emptyDataFrame

  private var _info: Map[String, Double] = Map(" " -> 0)

  val estimatorName: String = "ResnikSimilarityEstimator"
  val estimatorMeasureType: String = "similarity"

  /**
   * This udf maps a uri to its information content value
   */
  protected val mapper = udf((thing: String) => {
    _info.get(thing)
  })

  /**
   * This function takes a list of parents for two entities a and b and returns their Resnik similarity value
   * and the time it took to calculate that value
   * @param a List of parents for entity a
   * @param b List of parents for entity b
   * @return 2-Tuple of Resnik value and time taken
   */
  def resnikMethod(a: List[String], b: List[String]): Tuple2[Double, Double] = {
    if (a.isEmpty || b.isEmpty) {
      // Timekeeping
      val t_diff = t_net/1000
      return (0.0, t_diff)
    }
    else {
      // Timekeeping
      val t2 = System.currentTimeMillis()

      // main calculations
      val inter: List[String] = a.intersect(b)
      val cont: List[Double] = inter.map(_info(_))

      var maxIC: Double = 0.0
      if (cont.nonEmpty) {
        maxIC = cont.max
      } else {
        maxIC = 0.0
      }

      // Timekeeping
      val t3 = System.currentTimeMillis()
      val t_diff = (t_net + t3 - t2)/1000

      // return value
      return (maxIC, t_diff)
    }
  }

  /**
   * udf to invoke the ResnikMethod with the correct parameters
   */
  protected val resnik = udf((a: ofRef[String], b: ofRef[String]) => {
    /* a.keySet.intersect(b.keySet).map(k => k->(a(k),b(k))). */
    resnikMethod(a.toList, b.toList)
  })

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
   * This method sets the feature Dataframe for this Model
   * @param features target Dataframe with pairs of entities
   * @return the Resnik model
   */
  def setFeatures(features: DataFrame): this.type = {
    if ({
      var test = true
      for (inputCol <- _inputCols) {
        if (!features.columns.contains(inputCol)) {
          test = false
        }
      }
      test
    }) {
      _features = features.select(_inputCols.map(col): _*)
      this
    }
    else {
      throw new Exception("Features DataFrame must contain " + _inputCols.mkString(", ") + " columns")
    }
  }

  /**
   * Takes read in dataframe, and target dataframe and produces a dataframe with similarity values
   * @param dataset a dataframe read in over sansa rdf layer
   * @return a dataframe with four columns, two for the entities, one for the similarity value and one for the time
   */
  def transform (dataset: Dataset[_]): DataFrame = {
    // timekeeping
    val t0 = System.currentTimeMillis()

    var target: DataFrame = spark.emptyDataFrame

    if (_features.isEmpty) {
      // parent calculation
      val featureExtractorModel = new FeatureExtractorEval()
        .setMode("par").setDepth(_depth)
        .setTarget(_target.drop("entityA")
          .withColumnRenamed("entityB", "uri")
          .union(_target.drop("entityB")
            .withColumnRenamed("entityA", "uri"))
          .distinct())
      _parents = featureExtractorModel
        .transform(dataset)

      val bparents: DataFrame = _parents.groupBy("entity")
        .agg(collect_list("parent"))
        .withColumnRenamed("collect_list(parent)", "parents")
      // bparents.show(false)
      // bparents.printSchema()

      // _target.show(false)
      target = _target.join(bparents, _target("entityA") === bparents("entity"))
        .drop("entity")
        .withColumnRenamed("parents", "featuresA")
        .join(bparents, _target("entityB") === bparents("entity"))
        .drop("entity")
        .withColumnRenamed("parents", "featuresB")
      // target.show(false)

      // information content calculation
      // TODO: maybe rewrite this for bigger data
      _info = featureExtractorModel.setMode("ic")
        .transform(dataset).rdd.map(x => (x.getString(0), x.getDouble(1))).collectAsMap()
    } else {
      _parents = _features.select(_inputCols(0), _inputCols(1))
      val bparents: DataFrame = _parents.groupBy(_inputCols(0))
        .agg(collect_list("parent"))
        .withColumnRenamed("collect_list(parent)", "parents")

      target = _target.join(bparents, _target("entityA") === bparents(_inputCols(0)))
        .drop(_inputCols(0))
        .withColumnRenamed("parents", "featuresA")
        .join(bparents, _target("entityB") === bparents(_inputCols(0)))
        .drop(_inputCols(0))
        .withColumnRenamed("parents", "featuresB")
      // target.show(false)

      // information content calculation
      // TODO: maybe rewrite this for bigger data
      _info = _features.select(_inputCols(0), _inputCols(2)).rdd.map(x => (x.getString(0), x.getDouble(1))).collectAsMap()
    }

    // timekeeping
    val t1 = System.currentTimeMillis()
    t_net = t1 - t0

    target.count()

    val result = target.withColumn("ResnikTemp", resnik(col("featuresA"), col("featuresB")))
      .drop("featuresA", "featuresB")
    val temp = result.withColumn("Resnik", result("ResnikTemp._1"))
      .withColumn("ResnikTime", result("ResnikTemp._2"))
      .drop("ResnikTemp")

    val t4 = System.currentTimeMillis()
    val t: Double = (t4.toDouble-t0.toDouble)/1000.0

    temp.withColumn("ResnikFullTime", lit(t))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  override val uid: String = "FIXME"
}
