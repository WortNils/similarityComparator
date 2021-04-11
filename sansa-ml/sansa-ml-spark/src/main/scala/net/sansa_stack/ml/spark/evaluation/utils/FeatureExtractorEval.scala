package net.sansa_stack.ml.spark.evaluation.utils

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._


/**
 * This class creates from a dataset of triples features for Similarity Models
 */
class FeatureExtractorEval extends Transformer {
  val spark = SparkSession.builder.getOrCreate()
  private val _availableModes = Array("par", "par2", "ic", "root", "feat")
  private var _mode: String = "par"
  private var _depth: Int = 1
  private var _outputCol: String = "extractedFeatures"

  import spark.implicits._

  private var _target: DataFrame = Seq("0", "1").toDF()
  private var _root: String = "root"

  private var _data: Array[(String, String)] = Seq(("", "")).toArray

  var overall: Double = 0

  protected val divideBy = udf((value: Double) => {
    value/overall
  })

  protected val doubleBreadth = udf((a: String, b: String) => {
    var Q_a = ArrayBuffer(String)
    var Q_b = ArrayBuffer(String)

    var marked_a = ArrayBuffer((String, Integer))
    var marked_b = ArrayBuffer((String, Integer))

    Q_a.append(a)
    Q_b.append(b)

    var i: Int = 0
    marked_a.append((a, i))
    marked_b.append((b, i))

    var res = ""

    breakable {
      while (Q_a.nonEmpty || Q_b.nonEmpty) {
        i += 1

        // take first element of Queue a
        val node = Q_a(0)
        Q_a = Q_a.drop(1)

        // take first element of Queue b
        val node2 = Q_b(0)
        Q_b = Q_b.drop(1)

        // if node is marked break
        if (marked_b.exists(y => {y._1 == node})) {
          res = node
          break
        }
        /*
        marked_b.foreach(t => {
          if (t._1 == node) {
            res = node
            break
          }
        })
        */

        // if node2 is marked break
        if (marked_b.exists(y => {y._1 == node})) {
          res = node
          break
        }

        /*
        marked_a.foreach(t => {
          if (t._1 == node2) {
            res = node2
            break
          }
        })
        */

        // append node and node2 children to the Queues
        _data.foreach(t => {
          if (t._1 == node) {
            Q_a.append(t._2)
            marked_a.append((t._2, i))

          } else if (t._1 == node2) {
            Q_b.append(t._2)
            marked_b.append((t._2, i))
          }
        })
      }
      (res, 0.0)
    }
    (res, (marked_b(marked_b.indexWhere(t => {t._1 == res}))._2
      + marked_a(marked_a.indexWhere(t => {t._1 == res}))._2).toDouble)
  })

  /**
   * This method changes the features to be extracted
   * @param mode a string specifying the mode
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
    if (depth > 0) {
      _depth = depth
      this
    }
    else {
      throw new Exception("Depth must be at least 1.")
    }
  }

  /**
   * This method sets the root parameter for the isA-based measures
   * @param root a string specifying the root node of a graph
   * @return returns the FeatureExtractor
   */
  def setRoot(root: String): this.type = {
    _root = root
    this
  }

  /**
   * This method sets the target Dataframe for the features
    * @param target a Dataframe specifying the target uris
   * @return returns the FeatureExtractor
   */
  def setTarget(target: DataFrame): this.type = {
    _target = target
    this
  }

  /**
   * Takes read in dataframe and produces a dataframe with features
    * @param dataset a dataframe read in over sansa rdf layer
   * @return a dataframe with two columns, one for string of URI and one of a list of features
   */
  def transform(dataset: Dataset[_]): DataFrame = {
    import spark.implicits._

    val ds: Dataset[(String, String, String)] = dataset.as[(String, String, String)]

    val rawFeatures: Dataset[(String, String)] = _mode match {
      case "par" | "par2" | "root" | "path" => ds.flatMap(t => Seq(
        (t._3, t._1)))
      case "ic" | "feat" => ds.flatMap(t => Seq(
        (t._1, t._3),
        (t._3, t._1)))
      case _ => throw new Exception(
        "This mode is currently not supported .\n " +
          "You selected mode " + _mode + " .\n " +
          "Currently available modes are: " + _availableModes)
    }
    val returnDF: DataFrame = _mode match {
      case "par" =>
        val parents: DataFrame = rawFeatures.toDF()
        var right: DataFrame = parents.toDF(parents.columns.map(_ + "_R"): _*)
        var new_parents: DataFrame = _target.join(parents, _target("uri") === parents("_1")).drop("uri")
        var token: Long = new_parents.count()
        breakable {for (i <- 1 to _depth) {
          // join the data with itself then add these rows to the original data
          new_parents = new_parents.union(new_parents.join(right, new_parents("_2") === right("_1_R"))
            .drop("_2", "_1_R")).distinct()
          val temp: Long = new_parents.count()
          println(temp)

          // if the length of the dataframe is the same as in the last iteration break the loop
          if (temp == token) {
            break
          }
          token = temp
        }}
        new_parents.withColumnRenamed("_1", "entity")
          .withColumnRenamed("_2", "parent")
        // add join with target
        // target.withColumn("parents", parent(col("_1"), rawFeatures))
      case "par2" | "root" =>
        val parents: DataFrame = rawFeatures.toDF()
        var right: DataFrame = parents.drop("depth").toDF(parents.columns.map(_ + "_R"): _*)
        var new_parents: DataFrame = _target.join(parents, _target("uri") === parents("_1"))
          .drop("uri").withColumn("depth", lit(1))

        var token: Long = new_parents.count()

        breakable {for (i <- 1 to _depth) {
          // join the data with itself then add these rows to the original data
          right = new_parents.drop("depth").toDF(parents.columns.map(_ + "_R"): _*)
          new_parents = new_parents.union(new_parents.drop("depth").join(right, new_parents("_2") === right("_1_R"))
            .drop("_2", "_1_R").withColumn("depth", lit(i + 1))).distinct()
          val temp: Long = new_parents.count()

          // if the length of the dataframe is the same as in the last iteration break the loop
          if (temp == token) {
            break
          }
          token = temp
        }}
        new_parents.withColumnRenamed("_1", "entity")
          .withColumnRenamed("_2", "parent")
      case "ic" =>
        overall = rawFeatures.count()/2
        val count: DataFrame = rawFeatures.groupBy("_1").count()

        val info: DataFrame = count.withColumn("informationContent", divideBy(count("count")))
          .drop("count").withColumnRenamed("_1", "entity")
        // val info = target.join(count, count("_1") == target("_1"), "left")
        info
      case "feat" =>
        val filteredFeaturesDataFrame = _target
          .join(rawFeatures, rawFeatures("_1") === _target("uri"))
          .drop("_1")
          .groupBy("uri")
          .agg(collect_list("_2"))
          .withColumnRenamed("collect_list(_2)", "extractedFeatures")

        val cvModel: CountVectorizerModel = new CountVectorizer()
          .setInputCol("extractedFeatures")
          .setOutputCol("vectorizedFeatures")
          .fit(filteredFeaturesDataFrame)

        val tmpCvDf: DataFrame = cvModel.transform(filteredFeaturesDataFrame)
        // val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 }, DataTypes.BooleanType)
        val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 })
        val countVectorizedFeaturesDataFrame: DataFrame = tmpCvDf.filter(isNoneZeroVector(col("vectorizedFeatures"))).select("uri", "vectorizedFeatures").cache()
        countVectorizedFeaturesDataFrame
      case "path" =>
        val target = _target
        // two simultaneous breadth-first searches
        _data = rawFeatures.collect()
        _target.withColumn("pathdist", doubleBreadth(col("entityA"), col("entityB")))
      case _ => throw new Exception(
        "This mode is currently not supported .\n " +
          "You selected mode " + _mode + " .\n " +
          "Currently available modes are: " + _availableModes)
    }
    returnDF
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  override val uid: String = "FIXME"
}
