package net.sansa_stack.ml.spark.evaluation.models

import net.sansa_stack.ml.spark.evaluation.utils.FeatureExtractorEval
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.WrappedArray.ofRef
import scala.collection.Map

/**
 * This class takes a base dataset and a target DataFrame and returns the Wu and Palmer similarity value
 * and the time it took to arrive at that value in a DataFrame
 */
class WuAndPalmerModel extends Transformer with SimilarityModel{
  override val spark: SparkSession = SparkSession.builder.getOrCreate()
  import spark.implicits._
  private val _availableModes = Array("join", "path", "breadth")
  private var _mode: String = "join"
  private var _depth: Int = 1

  private var _inputCols: Array[String] = Array("entity", "parent", "depth", "rootDist")
  private var _outputCol: String = "extractedFeatures"

  private var _rootdist: Map[String, Int] = Map("ab" -> 2)
  private var _rootdist2: Map[String, Double] = Map("ab" -> 2.0)
  private var _max: Long = 0

  override val estimatorName: String = "WuAndPalmerSimilarityEstimator"
  override val estimatorMeasureType: String = "similarity"

  private var t_net: Double = 0.0

  protected var _target: DataFrame = spark.emptyDataFrame
  protected var _parents: DataFrame = spark.emptyDataFrame
  protected var _features: DataFrame = spark.emptyDataFrame

  /**
   * This method takes two lists of parents and their distance to the entity and returns the Wu and Palmer similarity value
   * @param a List of parents and their distance to entity a
   * @param b List of parents and their distance to entity b
   * @return 2-Tuple of Wu and Palmer value and time taken
   */
  def WuAndPalmerMethod(a: List[(String, Int)], b: List[(String, Int)]): Tuple2[Double, Double] = {
    if (a.isEmpty || b.isEmpty) {
      // Timekeeping
      val t_diff = t_net/1000
      (0.0, t_diff)
    }
    else {
      // Timekeeping
      val t2 = System.currentTimeMillis()

      // main calculations
      val a2 = a.toMap
      val b2 = b.toMap

      val c = a2.keySet.intersect(b2.keySet)

      if (c.isEmpty) {
        // Timekeeping
        val t3 = System.currentTimeMillis()
        val t_diff = (t_net + t3 - t2)/1000

        // return value
        return (0.0, t_diff)
        // return maxIC
      }

      val c2 = c.map(key => (key, a2(key) + b2(key)))
      val min = c2.minBy(_._2)

      val minDist: Int = min._2
      var ccc = new Array[String](1)
      ccc(0) = min._1
      var n = new Array[Int](1)

      try {
        n = ccc.map(_rootdist(_))
      } catch {
        case f: NoSuchElementException => n(0) = 0 // used to be max.toInt
      }
      val temp: Int = 2*n(0)

      val wupalm: Double = temp.toDouble / (minDist.toDouble + temp.toDouble)

      // Timekeeping
      val t3 = System.currentTimeMillis()
      val t_diff = (t_net + t3 - t2)/1000

      // return value
      (wupalm, t_diff)
    }
  }

  /**
   * udf to set Wu and Palmer similarity
   */
  protected val wuandpalmerbreadth = udf((m: Int, parent: String) => {
    // Timekeeping
    val t2 = System.currentTimeMillis()

    var n = new Array[Int](1)
    val ccc = new Array[String](1)
    ccc(0) = parent

    try {
      n = ccc.map(_rootdist(_))
    } catch {
      case f: NoSuchElementException => n(0) = 0 // used to be max.toInt
    }

    var wupalm: Double = 0

    if (m >= 0) {
      wupalm = (2 * n(0)).toDouble / (m.toDouble + (2 * n(0)).toDouble)
    }

    // Timekeeping
    val t3 = System.currentTimeMillis()
    val t_diff = (t_net + t3 - t2)/1000

    (wupalm, t_diff)
  })

  /**
   * udf to invoke the WuAndPalmerMethod with the correct parameters
   */
  protected val wuandpalmer = udf((a: ofRef[Row], b: ofRef[Row]) => {
    /* a.keySet.intersect(b.keySet).map(k => k->(a(k),b(k))). */
    // TODO: insert case for null
    val a2 = a.map({
      case Row(parent: String, depth: Int) =>
        (parent, depth)
    })
    val b2 = b.map({
      case Row(parent: String, depth: Int) =>
        (parent, depth)
    })
    WuAndPalmerMethod(a2.toList, b2.toList)
  })

  /**
   * udf to turn two columns into one tuple column
   */
  protected val toTuple = udf((par: String, depth: Int) => {
    Tuple2(par, depth)
  })

  /**
   * udf to turn a tuple column into a single Int column
   */
  protected val fromTuple = udf((thing: (String, Int)) => {
    thing._2
  })

  /**
   * udf to turn a tuple column into a single String column
   */
  protected val fromTuple_1 = udf((thing: (String, Int)) => {
    thing._1
  })

  /**
   * This method sets the iteration depth for the parent feature extraction
   * @param depth Integer value indicating how deep parents are searched for
   * @return the Wu and Palmer model
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
   * @return the Wu and Palmer model
   */
  def setTarget(target: DataFrame): this.type = {
    _target = target
    this
  }

  /**
   * This method changes what algorithm is used for calculating the similarity measure
   * @param mode a string specifying the mode
   * @return returns the Wu and Palmer model
   */
  def setMode(mode: String): this.type = {
    if (_availableModes.contains(mode)) {
      _mode = mode
      this
    }
    else {
      throw new Exception("The specified mode: " + mode + " is not supported. Currently available are: " + _availableModes.mkString("Array(", ", ", ")"))
    }
  }

  /**
   * Insert features into the model with a uri column, a parent column and the information
   * content of the parent in another column
   * @param features DataFrame with the data
   * @param uri name of the uri column
   * @param parent name of the parent column
   * @param depth name of the depth column
   * @param rootDist name of the root distance column
   * @return the WuAndPalmer model
   */
  def setFeatures(features: DataFrame, uri: String, parent: String, depth: String, rootDist: String): this.type = {
    if (!features.isEmpty) {
      _features = features.select(uri, parent, depth, rootDist)
        .withColumnRenamed(uri, _inputCols(0))
        .withColumnRenamed(parent, _inputCols(1))
        .withColumnRenamed(depth, _inputCols(2))
        .withColumnRenamed(rootDist, _inputCols(3))
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
      if (_mode == "join") {
        // timekeeping
        val t0 = System.currentTimeMillis()

        // parent calculation
        val featureExtractorModel = new FeatureExtractorEval()
          .setMode("par2").setDepth(_depth).setTarget(_target.drop("entityA")
          .withColumnRenamed("entityB", "uri")
          .union(_target.drop("entityB")
            .withColumnRenamed("entityA", "uri"))
          .distinct())
        _parents = featureExtractorModel
          .transform(dataset)

        val bparents: DataFrame = _parents
          .withColumn("parent2", toTuple(col("parent"), col("depth")))
          .drop("parent", "depth")
          .groupBy("entity")
          .agg(collect_list("parent2"))
          .withColumnRenamed("collect_list(parent2)", "parents")
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
        // target.printSchema()

        /*
        val rooter = _parents.filter(_parents("parent2._1") === _root)
          .withColumn("rootdist", fromTuple(col("parent2")))
          .drop("parent2")
         */
        val rents = _parents.drop("entity", "depth").distinct().withColumnRenamed("parent", "uri")
        val rooter = featureExtractorModel.setMode("par2").setDepth(_depth).setTarget(rents).transform(dataset)
        val roots = rooter.groupBy("entity")
          .agg(max(rooter("depth")))
          .withColumnRenamed("max(depth)", "rootdist")

        // rooter.show(false)

        _rootdist = roots.rdd.map(x => (x.getString(0), x.getInt(1))).collectAsMap()
        // println(_rootdist)
        _max = dataset.count()
        // target.where($"featuresB".isNull).show(false)
        /* val valueM1 = _parents.filter(_parents("entity") === "urn:m1")
        valueM1.show(false) */


        // timekeeping
        val t1 = System.currentTimeMillis()
        t_net = t1 - t0

        val result = target.withColumn("WuAndPalmerTemp", wuandpalmer(col("featuresA"), col("featuresB")))
          .drop("featuresA", "featuresB")
        val temp = result.withColumn("WuAndPalmer", result("WuAndPalmerTemp._1"))
          .withColumn("WuAndPalmerTime", result("WuAndPalmerTemp._2"))
          .drop("WuAndPalmerTemp")

        val t4 = System.currentTimeMillis()
        val t_ = (t4-t0)/1000

        temp.withColumn("WuAndPalmerFullTime", lit(t_))
      }
      else if (_mode == "path") {
        // timekeeping
        val t0 = System.currentTimeMillis()

        // find all nodes
        val ds: Dataset[(String, String, String)] = dataset.toDF().as[(String, String, String)]
        val data = ds.flatMap(t => Seq(t._1, t._3)).distinct().toDF()
          .withColumnRenamed("value", "uri")

        // parent calculation
        val featureExtractorModel = new FeatureExtractorEval()
          .setMode("apsp")
          .setDepth(_depth)
          .setTarget(data)

        val apsp = featureExtractorModel.transform(dataset)

        // join for entityA and entityB
        val target: DataFrame = _target.join(apsp, _target("entityA") === apsp("uri"))
          .drop("uri")
          .withColumnRenamed("parents", "featuresA")
          .join(apsp, _target("entityB") === apsp("uri"))
          .drop("uri")
          .withColumnRenamed("parents", "featuresB")

        // find longest paths for every entity
        val roots = apsp
          .groupBy("entity")
          .agg(max(apsp("depth")))
          .withColumnRenamed("max(depth)", "rootdist")

        _rootdist = roots.rdd.map(x => (x.getString(0), x.getInt(1))).collectAsMap()
        _max = dataset.count()

        // timekeeping
        val t1 = System.currentTimeMillis()
        t_net = t1 - t0

        val result = target.withColumn("WuAndPalmerTemp", wuandpalmer(col("featuresA"), col("featuresB")))
          .drop("featuresA", "featuresB")
        val temp = result.withColumn("WuAndPalmer", result("WuAndPalmerTemp._1"))
          .withColumn("WuAndPalmerTime", result("WuAndPalmerTemp._2"))
          .drop("WuAndPalmerTemp")

        val t4 = System.currentTimeMillis()
        val t_ = (t4-t0)/1000

        target.count()

        temp.withColumn("WuAndPalmerFullTime", lit(t_))
      }
      else if (_mode == "breadth") {
        // timekeeping
        val t0 = System.currentTimeMillis()

        val featureExtractorModel = new FeatureExtractorEval()
          .setMode("path").setDepth(_depth).setTarget(_target)
        _parents = featureExtractorModel
          .transform(dataset)

        val target = _parents.withColumn("dist", fromTuple(col("pathdist")))
          .withColumn("parent", fromTuple_1(col("pathdist")))

        val renter = _parents.withColumn("uri", fromTuple_1(col("pathdist")))
          .drop("entityA", "entityB", "pathdist")
          .distinct()

        val rents = renter.where(renter("uri") =!= "")

        val rooter = featureExtractorModel.setMode("root").setDepth(_depth).setTarget(rents).transform(dataset)

        val roots = rooter.groupBy("entity")
          .agg(max(rooter("depth")))
          .withColumnRenamed("max(depth)", "rootdist")

        _rootdist = roots.rdd.map(x => (x.getString(0), x.getInt(1))).collectAsMap()
        _max = dataset.count()

        // timekeeping
        val t1 = System.currentTimeMillis()
        t_net = t1 - t0

        val result = target.withColumn("WuAndPalmerTemp", wuandpalmerbreadth(col("dist"), col("parent")))
          .drop("pathdist", "dist", "parent")
        val temp = result.withColumn("WuAndPalmer", result("WuAndPalmerTemp._1"))
          .withColumn("WuAndPalmerTime", result("WuAndPalmerTemp._2"))
          .drop("WuAndPalmerTemp")

        val t4 = System.currentTimeMillis()
        val t_ = (t4-t0)/1000

        target.count()

        temp.withColumn("WuAndPalmerFullTime", lit(t_))
      }
      else {
        _target.withColumn("WuAndPalmer", lit(0))
          .withColumn("WuAndPalmerTime", lit(0))
          .withColumn("WuAndPalmerFullTime", lit(0))
      }
    }
    else {
      // timekeeping
      val t0 = System.currentTimeMillis()

      // parent calculation
      _parents = _features.select(_inputCols(0), _inputCols(1), _inputCols(2))

      val bparents: DataFrame = _parents
        .withColumn("parent2", toTuple(col("parent"), col("depth")))
        .drop("parent", "depth")
        .groupBy("entity")
        .agg(collect_list("parent2"))
        .withColumnRenamed("collect_list(parent2)", "parents")
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
      // target.printSchema()

      /*
      val rooter = _parents.filter(_parents("parent2._1") === _root)
        .withColumn("rootdist", fromTuple(col("parent2")))
        .drop("parent2")
       */
      val roots = _features.select(_inputCols(1), _inputCols(3)).where(_features(_inputCols(3)) =!= null).distinct()

      // rooter.show(false)

      _rootdist = roots.rdd.map(x => (x.getString(0), x.getInt(1))).collectAsMap()
      // println(_rootdist)
      _max = dataset.count()
      // target.where($"featuresB".isNull).show(false)
      /* val valueM1 = _parents.filter(_parents("entity") === "urn:m1")
      valueM1.show(false) */


      // timekeeping
      val t1 = System.currentTimeMillis()
      t_net = t1 - t0

      val result = target.withColumn("WuAndPalmerTemp", wuandpalmer(col("featuresA"), col("featuresB")))
        .drop("featuresA", "featuresB")
      val temp = result.withColumn("WuAndPalmer", result("WuAndPalmerTemp._1"))
        .withColumn("WuAndPalmerTime", result("WuAndPalmerTemp._2"))
        .drop("WuAndPalmerTemp")

      val t4 = System.currentTimeMillis()
      val t_ = (t4-t0)/1000

      temp.withColumn("WuAndPalmerFullTime", lit(t_))
    }
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  override val uid: String = "FIXME"
}
