package net.sansa_stack.ml.spark.evaluation.utils

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions.monotonicallyIncreasingId
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * This Class creates from a dataset of triples, a DataFrame that pairs two Uris according to given parameters
 */
class SimilaritySampler extends Transformer {
  val spark = SparkSession.builder.getOrCreate()
  private val _availableModes = Array("rand", "cross", "crossFull", "crossOld", "limit", "sparql")
  private var _mode: String = "cross"
  private val _outputCols: Array[String] = Array("entityA", "entityB")
  private var _seed: Long = 10
  private var _limit: Int = 1


  /**
   * This method changes how the entities are paired up
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
   * This method changes the seed to be used for randomized samplings
   * @param seed a Long specifying the random seed
   * @return returns the FeatureExtractor
   */
  def setSeed(seed: Long): this.type = {
    if (seed > 0) {
      _seed = seed
      this
    } else {
      throw new Exception("The specified amount was not negative.")
    }
  }

  /**
   * This method changes the amount of rows to take in limit mode
   * @param limit an Int specifiyng the amount of rows to take
   * @return returns the FeatureExtractor
   */
  def setLimit(limit: Int): this.type = {
    if (limit > 0) {
      _limit = limit
      this
    } else {
      throw new Exception("The specified amount was not negative.")
    }
  }

  /**
   * Takes read in dataset and produces a dataframe paired up entities
   * @param dataset a dataframe read in over sansa rdf layer
   * @return a dataframe with two columns, one for string of the first URI and one for the second
   */
  def transform(dataset: Dataset[_]): DataFrame = {
    import spark.implicits._

    val ds: Dataset[(String, String, String)] = dataset.as[(String, String, String)]

    val rawLit = ds.flatMap(t => Seq((t._1), (t._3))).distinct().toDF()
      .withColumnRenamed("value", "entityA")

    // dirty way to remove literals
    // val raw = rawLit.where(rawLit("entityA").contains("http://"))
    val raw = rawLit

    val rawDF: DataFrame = _mode match {
      case "cross" | "sparql" =>
        val tempDf = raw.crossJoin(raw.withColumnRenamed("entityA", "entityB"))
        tempDf.where(tempDf("entityA") >= tempDf("entityB"))
      case "crossFull" =>
        raw.crossJoin(raw.withColumnRenamed("entityA", "entityB"))
      case "crossOld" =>
        // dirty solution with deprecated function
        val tempDf = raw.withColumn("idA", monotonicallyIncreasingId)
          .crossJoin(raw.withColumnRenamed("entityA", "entityB")
            .withColumnRenamed("idA", "idB"))
        tempDf.where(tempDf("idA") >= tempDf("idB")).drop("idA", "idB")
      case "rand" =>
        val rawt = raw.sample(withReplacement = true, fraction = 0.002, seed = _seed)
        val tempDf = rawt.crossJoin(rawt.withColumnRenamed("entityA", "entityB"))
        tempDf.where(tempDf("entityA") >= tempDf("entityB"))
      case "limit" =>
        val tempDf: DataFrame = raw.limit(_limit)
        val retDf: DataFrame = tempDf.crossJoin(tempDf.withColumnRenamed("entityA", "entityB"))
        retDf.where(retDf("entityA") >= retDf("entityB"))
      case "sparql" =>
        // sample according to a sparql query
        raw
    }

    rawDF
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  override val uid: String = "FIXME"
}
