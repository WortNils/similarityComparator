package net.sansa_stack.ml.spark.evaluation.utils

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.control.Breaks._


/**
 * This class creates from a dataset of triples features for Similarity Models
 */
class FeatureExtractorEval extends Transformer {
  val spark = SparkSession.builder.getOrCreate()
  private val _availableModes = Array("par", "ic")
  private var _mode: String = "par"
  private var _depth: Int = 1
  private var _outputCol: String = "extractedFeatures"

  var overall: Double = 0

  /**
   * This method changes the features to be extracted
   * @param mode a string specifying the mode. Modes are abbreviations of their corresponding models
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
    if (depth > 1) {
      _depth = depth
      this
    }
    else {
      throw new Exception("Depth must be at least 1.")
    }
  }

  /**
   * private method for depth-search of parent nodes
   * @param parents DataFrame of depth 0 parents
   * @param data full scope DataFrame
   * @return returns a DataFrame of all parents up to _depth
   */
  /* private def findParents(parents: DataFrame, data: DataFrame): DataFrame = {
    import spark.implicits._
    val search: DataFrame = parents
    data.explode()
    for (i <- 1 to _depth) {
       while (!search.isEmpty) {
         search.foreach{row =>
           parents = parents.union(data.filter(data("uri") === row(0))) p = p u {parents(search)}
           search = search.filter($"uri" != row(0))
           // TODO: find out what is wrong with filter
           /* val parent = udf((row: Row) => {
             val l = row.toSeq.toList
             val firstElement = l(0)

           })
           // filter out cyclic paths from currentPaths
           val noCycle = udf((row: Row) => {
             val l = row.toSeq.toList
               .filter(_!=None)
               .filter(_!=null)
             val lFromSet = l.toSet
             l.length == lFromSet.size
           })
           val nNamedColumns = currentPaths.columns.filter(_.startsWith("n_")).toList */
         }
       }
    }
  } */

  /*
  protected val parent = udf((data: Dataset[(String, String)]) => {
    // TODO: redo with map maybe?

  }) */

  protected val divideBy = udf((value: Double) => {
    value/overall
  })

  /**
   * Takes read in dataframe and produces a dataframe with features
    * @param dataset a dataframe read in over sansa rdf layer
   * @return a dataframe with two columns, one for string of URI and one of a list of features
   */
  def transform(dataset: Dataset[_]): DataFrame = {
    import spark.implicits._

    val ds: Dataset[(String, String, String)] = dataset.as[(String, String, String)]

    val rawFeatures: Dataset[(String, String)] = _mode match {
      case "par" => ds.flatMap(t => Seq(
        (t._3, t._1)))
      case "ic" => ds.flatMap(t => Seq(
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
        var new_parents: DataFrame = parents
        var token: Long = new_parents.count()
        breakable {for (i <- 1 to _depth) {
          // join the data with itself then add these rows to the original data
          right = new_parents.toDF(new_parents.columns.map(_ + "_R"): _*)
          new_parents = new_parents.union(new_parents.join(right, new_parents("_2") === right("_1_R"))
            .drop("_2", "_1_R")).distinct()
          val temp: Long = new_parents.count()

          // if the length of the dataframe is the same as in the last iteration break the loop
          if (temp == token) {
            break
          }
          token = temp
        }}
        new_parents
        // add join with target
        // target.withColumn("parents", parent(col("_1"), rawFeatures))
      case "ic" =>
        overall = rawFeatures.count()/2
        val count: DataFrame = rawFeatures.groupBy("_1").count()
        val info: DataFrame = count.withColumn("informationContent", divideBy(count("count")))
          .drop("count").withColumnRenamed("_1", "entity")
        // val info = target.join(count, count("_1") == target("_1"), "left")
        info
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
