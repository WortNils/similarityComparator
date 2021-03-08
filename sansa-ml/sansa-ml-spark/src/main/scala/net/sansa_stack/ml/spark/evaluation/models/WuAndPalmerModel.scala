package net.sansa_stack.ml.spark.evaluation.models

import net.sansa_stack.ml.spark.evaluation.utils.FeatureExtractorEval
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import scala.collection.mutable.WrappedArray.ofRef

import scala.collection.Map

class WuAndPalmerModel extends Transformer{
  val spark = SparkSession.builder.getOrCreate()
  import spark.implicits._
  private val _availableModes = Array("res")
  private var _mode: String = "res"
  private var _depth: Int = 1
  private var _outputCol: String = "extractedFeatures"

  val estimatorName: String = "ResnikSimilarityEstimator"
  val estimatorMeasureType: String = "similarity"

  private var t_net: Double = 0.0

  private var _target: DataFrame = Seq("0", "1").toDF()
  private var _parents: DataFrame = Seq("0", "1").toDF()

  def transform (dataset: Dataset[_]): DataFrame = {
    // timekeeping
    val t0 = System.nanoTime()

    // parent calculation
    val featureExtractorModel = new FeatureExtractorEval()
      .setMode("par").setDepth(_depth)
    _parents = featureExtractorModel
      .transform(dataset)

    val bparents: DataFrame = _parents.groupBy("entity")
      .agg(collect_list("parent"))
      .withColumnRenamed("collect_list(parent)", "parents")
    // bparents.show(false)
    // bparents.printSchema()

    // _target.show(false)
    val target: DataFrame = _target.join(bparents, _target("entityA") === bparents("entity"))
      .drop("entity")
      .withColumnRenamed("parents", "featuresA")
      .join(bparents, _target("entityB") === bparents("entity"))
      .drop("entity")
      .withColumnRenamed("parents", "featuresB")
    // target.show(false)

    // target.where($"featuresB".isNull).show(false)

    // information content calculation
    // TODO: maybe rewrite this for bigger data
    _info = featureExtractorModel.setMode("ic")
      .transform(dataset).rdd.map(x => (x.getString(0), x.getDouble(1))).collectAsMap()

    // timekeeping
    val t1 = System.nanoTime()
    t_net = t1 - t0

    target.withColumn("Resnik", resnik(col("featuresA"), col("featuresB"))).drop("featuresA", "featuresB")
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  override val uid: String = "FIXME"
}
