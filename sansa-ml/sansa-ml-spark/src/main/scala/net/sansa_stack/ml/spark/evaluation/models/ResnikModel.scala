package net.sansa_stack.ml.spark.evaluation.models

import net.sansa_stack.ml.spark.evaluation.utils.FeatureExtractorEval
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import scala.collection.mutable.WrappedArray.ofRef

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

  def resnikMethod(a: List[String], b: List[String]): Tuple2[Double, Double] = {
    if (a.isEmpty || b.isEmpty) {
      // Timekeeping
      val t_diff = t_net/1000000000
      return (0, t_diff)
    }
    else {
      // Timekeeping
      val t2 = System.nanoTime()

      // val _a = a.get
      // val _b = b.get

      // main calculations
      val inter: List[String] = a.intersect(b)
      val cont: List[Double] = inter.map(_info(_))

      /* ideas to counter "Dataset transformations and actions can only be invoked by the driver,
         not inside of other Dataset transformations":
         - turn parents into map
         - two joins on the target data to add the parents as feature columns */

      /* val inter: DataFrame = _parents.filter($"entity" === a).drop("entity")
        .intersect(_parents.filter($"entity" === b).drop("entity"))
      // inter.show(false)

      val cont: DataFrame = inter.withColumn("IC", mapper(col("parent")))
      // cont.show(false) */
      var maxIC: Double = 0.0
      if (cont.nonEmpty) {
        maxIC = cont.max
      } else {
        maxIC = 0.0
      }

      // Timekeeping
      val t3 = System.nanoTime()
      val t_diff = (t_net + t3 - t2)/1000000000

      // val maxIC: Double = cont.select("IC").orderBy(col("IC").desc).head().getDouble(0)
      // cont.select("IC").collect().max
      // cont.select("IC").agg(max($"IC"))
      // cont.select("IC").rdd.max()[0][0]

      // return value
      return (maxIC, t_diff)
    }
  }

  protected val resnik = udf((a: ofRef[String], b: ofRef[String]) => {
    /* a.keySet.intersect(b.keySet).map(k => k->(a(k),b(k))). */
    resnikMethod(a.toList, b.toList)
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
