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

  private var _root: String = "root"
  private var _rootdist: Map[String, Int] = Map("ab" -> 2)
  private var _max: Long = 0

  val estimatorName: String = "WuAndPalmerSimilarityEstimator"
  val estimatorMeasureType: String = "similarity"

  private var t_net: Double = 0.0

  private var _target: DataFrame = Seq("0", "1").toDF()
  private var _parents: DataFrame = Seq("0", "1").toDF()

  def WuAndPalmerMethod(a: List[(String, Int)], b: List[(String, Int)]): Tuple2[Double, Double] = {
    if (a.isEmpty || b.isEmpty) {
      // Timekeeping
      val t_diff = t_net/1000000000
      return (0.0, t_diff)
      // return 0.0
    }
    else {
      // Timekeeping
      val t2 = System.nanoTime()

      var a2 = new Array[String](a.length)
      var b2 = new Array[String](b.length)
      // main calculations
      for (i <- 0 to a.length) {
        a2(i) = a(i)._1
      }
      for (i <- 0 to b.length) {
        b2(i) = b(i)._1
      }

      val inter: Array[String] = a2.intersect(b2)

      if (inter.isEmpty) {
        val t3 = System.nanoTime()
        val t_diff = (t_net + t3 - t2)/1000000000
        return (0.0, t_diff)
        // return 0.0
      }

      var dist = new Array[Int](inter.length)

      for (i <- 0 to inter.length) {
        val indA = a2.indexOf(inter(i))
        val indB = b2.indexOf(inter(i))

        dist(i) = a(indA)._2 + b(indB)._2
      }

      val minDist = dist.min
      val indMin = dist.indexOf(minDist)
      var ccc = new Array[String](1)
      ccc(0) = inter(indMin)
      var n = new Array[Int](1)

      try {
        n = ccc.map(_rootdist(_))
      } catch {
        case f: NoSuchElementException => n(0) = _max.toInt
      }

      val wupalm: Double = (2 * n(0)) / (minDist + 2 * n(0))

      // Timekeeping
      val t3 = System.nanoTime()
      val t_diff = (t_net + t3 - t2)/1000000000

      // return value
      return (wupalm, t_diff)
      // return maxIC
    }
  }

  protected val wuandpalmer = udf((a: ofRef[(String, Int)], b: ofRef[(String, Int)]) => {
    /* a.keySet.intersect(b.keySet).map(k => k->(a(k),b(k))). */
    WuAndPalmerMethod(a.toList, b.toList)
  })

  protected val toTuple = udf((par: String, depth: Int) => {
    Tuple2(par, depth)
  })

  protected val fromTuple = udf((thing: (String, Double)) => {
    thing._2
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

  def setRoot(root: String): this.type = {
    _root = root
    this
  }

  def setTarget(target: DataFrame): this.type = {
    _target = target
    this
  }

  def transform (dataset: Dataset[_]): DataFrame = {
    // timekeeping
    val t0 = System.nanoTime()

    // parent calculation
    val featureExtractorModel = new FeatureExtractorEval()
      .setMode("par2").setDepth(_depth)
    _parents = featureExtractorModel
      .transform(dataset)
      .withColumn("parent2", toTuple(col("parent"), col("depth")))

    val bparents: DataFrame = _parents.groupBy("entity")
      .agg(collect_list("parent2"))
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

    val rooter = _parents.filter(_parents("parent2")
      .contains(_root))
      .withColumn("rootdist", fromTuple(col("parent2")))
      .drop("parent2")

    _rootdist = rooter.rdd.map(x => (x.getString(0), x.getInt(1))).collectAsMap()
    _max = dataset.count()
    // target.where($"featuresB".isNull).show(false)

    // timekeeping
    val t1 = System.nanoTime()
    t_net = t1 - t0

    target.withColumn("WuAndPalmer", wuandpalmer(col("featuresA"), col("featuresB")))
      .drop("featuresA", "featuresB")
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  override val uid: String = "FIXME"
}
