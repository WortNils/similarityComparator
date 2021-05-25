package net.sansa_stack.ml.spark.evaluation.utils

import net.sansa_stack.ml.spark.featureExtraction.SparqlFrame
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

  private val _availableLiteralRemoval = Array("none", "http", "bool")
  private var _litMode = "none"

  private var _sparql = "SELECT ?s ?p ?o WHERE {?s ?p ?o}"

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
      throw new Exception("The specified mode: " + mode + " is not supported. Currently available are: " + _availableModes)
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
   * @return returns the SimilaritySampler
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
   * This method sets the way in which literals are removed
   * @param litMode a String specifying the literal removal mode
   * @return returns the SimilaritySampler
   */
  def setLiteralRemoval(litMode: String): this.type = {
    if (_availableLiteralRemoval.contains(litMode)) {
      _litMode = litMode
      this
    }
    else {
      throw new Exception("The specified mode: " + litMode + " is not supported. Currently available are: " + _availableLiteralRemoval)
    }
  }

  /**
   * This method takes the sparql query for the sparql mode
   * @param sparql a String specifying the sparql query in sparql mode
   * @return returns the SimilaritySampler
   */
  def setSPARQLQuery(sparql: String): this.type = {
    // TODO: Add check for valid sparql query
    _sparql = sparql
    this
  }

  /**
   * Takes read in dataset and produces a dataframe paired up entities
   * @param dataset a dataframe read in over sansa rdf layer
   * @return a dataframe with two columns, one for string of the first URI and one for the second
   */
  def transform(dataset: Dataset[_]): DataFrame = {
    import spark.implicits._

    val literalRem = new LiteralRemover

    val lit = literalRem.setMode(_litMode).transform(dataset)

    val ds: Dataset[(String, String, String)] = lit.toDF().as[(String, String, String)]

    val raw = ds.flatMap(t => Seq((t._1), (t._3))).distinct().toDF()
      .withColumnRenamed("value", "entityA")

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
        val _queryString = _sparql
        val sparqlFrame = new SparqlFrame()
          .setSparqlQuery(_queryString)
        val res = sparqlFrame.transform(dataset)
        res
    }

    rawDF
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  override val uid: String = "FIXME"
}
