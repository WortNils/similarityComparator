package net.sansa_stack.ml.spark.evaluation.models

import net.sansa_stack.ml.spark.evaluation.utils.FeatureExtractorEval
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.Map

class ResnikModel extends Transformer {
  val spark = SparkSession.builder.getOrCreate()
  import spark.implicits._
  private val _availableModes = Array("res")
  private var _mode: String = "res"
  private var _depth: Int = 1
  private var _outputCol: String = "extractedFeatures"

  private var t_net: Double = 0.0

  private var _target: DataFrame = Seq("0", "1").toDF()
  private var _parents: DataFrame = Seq("0", "1").toDF()

  private var _info: Map[String, Double] = Map("a" -> 2)

  protected val mapper = udf((thing: String) => {
    _info.get(thing)
  })

  protected val resnik = udf((a: String, b: String) => {
    /* a.keySet.intersect(b.keySet).map(k => k->(a(k),b(k))). */
    // Timekeeping
    val t2 = System.nanoTime()

    // main calculations
    /* val inter: List[String] = a.intersect(b)
    val cont: List[Double] = inter.map(info(_)) */

    val inter: DataFrame = _parents.filter($"entity" === a).drop("entity")
      .intersect(_parents.filter($"entity" === b).drop("entity"))
    // inter.show(false)

    val cont: DataFrame = inter.withColumn("IC", mapper(col("parent")))
    // cont.show(false)

    // Timekeeping
    val t3 = System.nanoTime()
    val t_diff = (t_net + t3 - t2)/1000000000

    val maxIC: Double = cont.select("IC").orderBy(col("IC").desc).head().getDouble(0)
    // cont.select("IC").collect().max
    // cont.select("IC").agg(max($"IC"))
    // cont.select("IC").rdd.max()[0][0]

    // return value
    (maxIC, t_diff)
  })

  def setDepth(depth: Int): this.type = {
    if (depth > 1) {
      _depth = depth
      this
    }
    else {
      throw new Exception("Depth must be at least 1.")
    }
  }

  def setTarget(target: DataFrame): this.type = {
    _target = target
    this
  }

  val estimatorName: String = "ResnikSimilarityEstimator"
  val estimatorMeasureType: String = "similarity"

  def transform (dataset: Dataset[_]): DataFrame = {
    val t0 = System.nanoTime()
    val featureExtractorModel = new FeatureExtractorEval()
      .setMode("par").setDepth(_depth)
    _parents = featureExtractorModel
      .transform(dataset)

    // maybe rewrite this for bigger data
    _info = featureExtractorModel.setMode("ic")
      .transform(dataset).rdd.map(x => (x.getString(0), x.getDouble(1))).collectAsMap()
    val t1 = System.nanoTime()
    t_net = t1 - t0

    _target.withColumn("Resnik", resnik(col("entityA"), col("entityB")))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  override val uid: String = "FIXME"
}
