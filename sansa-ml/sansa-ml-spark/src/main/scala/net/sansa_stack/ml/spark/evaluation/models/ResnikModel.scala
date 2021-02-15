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
  private val _availableModes = Array("res")
  private var _mode: String = "res"
  private var _depth: Int = 1
  private var _outputCol: String = "extractedFeatures"

  private var t_net: Double = 0.0

  private var _target: DataFrame = Seq(0).toDF()

  private var info: Map[String, Double] = Map("a" -> 2)

  protected val resnik = udf((a: List[String], b: List[String]) => {
    /* a.keySet.intersect(b.keySet).map(k => k->(a(k),b(k))). */
    // Timekeeping
    val t2 = System.nanoTime()

    // main calculations
    val inter: List[String] = a.intersect(b)
    val cont: List[Double] = inter.map(info(_))

    // Timekeeping
    val t3 = System.nanoTime()
    val t_diff = t_net + t3 - t2

    // return value
    (cont.max, t_diff)
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
    val parents: DataFrame = featureExtractorModel
      .transform(dataset)
    val info: Map[String, Double] = featureExtractorModel.setMode("ic").transform(dataset).rdd.map(x => (x.getString(0), x.getDouble(1))).collectAsMap()
    val t1 = System.nanoTime()
    t_net = (t1 - t0)

    /* frame.map{row: Row =>
      val a: DataFrame = parents.where(parents("uri") === row(0)).drop("uri").toDF
      val b: DataFrame = parents.where(parents("uri") === row(1)).drop("uri").toDF
      val common: DataFrame = a.intersect(b)

      featureExtractorModel.setMode("ic")
      val informationContent = featureExtractorModel
        .transform(dataset, common)
      val resnik = informationContent.sort(desc(columnName = "extractedFeatures")).head()
      val t2 = System.nanoTime()
      val t_diff = t1 - t0 + t2 - t1
      /* row("Resnik") = resnik
      row("ResnikTime") = t_diff */
      row
    }.toDF */

    _target.withColumn("Resnik", resnik(col("FeaturesA"), col("FeaturesB")))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  override val uid: String = "FIXME"
}
