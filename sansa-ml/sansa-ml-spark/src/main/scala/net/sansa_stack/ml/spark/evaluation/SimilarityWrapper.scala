package net.sansa_stack.ml.spark.evaluation

import com.mysql.cj.exceptions.WrongArgumentException
import net.sansa_stack.ml.spark.evaluation.models.ResnikModel
import net.sansa_stack.ml.spark.evaluation.utils.{FeatureExtractorEval, SimilaritySampler}
import org.apache.spark.sql.functions.{col, collect_list, max, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

class SimilarityWrapper {
  val spark: SparkSession = SparkSession.builder.getOrCreate()

  // wrapper variables
  private var res: Boolean = false
  private var wp: Boolean = false
  private var tver: Boolean = false

  // sampling variables
  private val _availableSamplingModes = Array("rand", "cross", "crossFull", "crossOld", "limit", "sparql")
  private var _samplingMode: String = "limit"
  private var _samplingLimit: Int = 10
  private var _samplingSeed: Long = 20
  private var _samplingSparql = "SELECT ?s ?p ?o WHERE {?s ?p ?o}"

  // literal removal variables
  private val _availableLiteralRemovalModes = Array("none", "http", "bool")
  private var _literalRemovalMode = "none"


  // sampling setter
  /**
   * This method changes how samples are taken
   * @param samplingMode a string specifying the mode
   * @return returns Wrapper
   */
  def setSamplingMode(samplingMode: String): this.type = {
    if (_availableSamplingModes.contains(samplingMode)) {
      _samplingMode = samplingMode
      this
    }
    else {
      throw new IllegalArgumentException("The specified mode: " + samplingMode + " is not supported. " +
        "Currently available are: " + _availableSamplingModes.mkString(", "))
    }
  }

  /**
   * This method changes the amount of rows to take in limit sampling mode
   * @param samplingLimit an Int specifying the amount of rows to take
   * @return returns Wrapper
   */
  def setSamplingLimit(samplingLimit: Int): this.type = {
    if (samplingLimit > 0) {
      _samplingLimit = samplingLimit
      this
    } else {
      throw new IllegalArgumentException("The specified amount was negative.")
    }
  }

  /**
   * This method changes the seed to be used for randomized samplings
   * @param seed a Long specifying the random seed
   * @return returns Wrapper
   */
  def setSamplingSeed(seed: Long): this.type = {
    if (seed > 0) {
      _samplingSeed = seed
      this
    } else {
      throw new IllegalArgumentException("The specified amount was negative.")
    }
  }

  /**
   * This method takes the sparql query for sparql sampling mode
   * @param sparql a String specifying the sparql query
   * @return returns Wrapper
   */
  def setSamplingSPARQLQuery(samplingSparql: String): this.type = {
    // TODO: Add check for valid sparql query
    _samplingSparql = samplingSparql
    this
  }


  // literal removal setter
  /**
   * This method sets the way in which literals are removed
   * @param litMode a String specifying the literal removal mode
   * @return returns Wrapper
   */
  def setLiteralRemoval(litMode: String): this.type = {
    if (_availableLiteralRemovalModes.contains(litMode)) {
      _literalRemovalMode = litMode
      this
    }
    else {
      throw new IllegalArgumentException("The specified mode: " + litMode + " is not supported. " +
        "Currently available are: " + _availableLiteralRemovalModes.mkString(", "))
    }
  }

  // udfs
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


  def evaluate (data: DataFrame,
                models: Array[String],
                iterationDepth: Int = 5,
                WP_mode: String = "join",
                T_alpha: Double = 1.0,
                T_beta: Double = 1.0): DataFrame = {
    // catching wrong arguments
    if (data.isEmpty) {
      throw new IllegalArgumentException("The target DataFrame is empty.")
    }
    if (!models.contains("Resnik") || !models.contains("WuAndPalmer") || !models.contains("Tversky")) {
      throw new IllegalArgumentException("The models array must contain at least one of Resnik, WuAndPalmer or Tversky.")
    }
    if (iterationDepth < 1) {
      throw new IllegalArgumentException("iterationDepth must be 1 at least.")
    }
    if (WP_mode != "join" || WP_mode != "breadth" || WP_mode != "path") {
      throw new IllegalArgumentException("WP_mode must be one of join, breadth or path")
    }
    if (T_alpha < 0.0 || T_alpha > 1.0) {
      throw new IllegalArgumentException("T_alpha must be between 0 and 1.")
    }
    if (T_beta < 0.0 || T_beta > 1.0) {
      throw new IllegalArgumentException("T_beta must be between 0 and 1.")
    }


    // turn the model array into booleans
    if (models.contains("Resnik")) {
      res = true
    }
    if (models.contains("WuAndPalmer")) {
      wp = true
    }
    if (models.contains("Tversky")) {
      tver = true
    }


    // sampling
    val sampler = new SimilaritySampler()
    val target: DataFrame = sampler
      .setMode(_samplingMode)
      .setLimit(_samplingLimit)
      .setSeed(_samplingSeed)
      .setLiteralRemoval(_literalRemovalMode)
      .transform(data)


    // feature Extraction
    val featureExtractorModel = new FeatureExtractorEval().setDepth(iterationDepth)

    val realTarget = target.drop("entityA")
      .withColumnRenamed("entityB", "uri")
      .union(target.drop("entityB")
        .withColumnRenamed("entityA", "uri"))
      .distinct()

    featureExtractorModel.setTarget(realTarget)

    // parents
    var parents = target

    if (wp && WP_mode == "join") {
      featureExtractorModel.setMode("par2")
      parents = featureExtractorModel.transform(data)
    }
    if (res && (!wp || WP_mode != "join")) {
      featureExtractorModel.setMode("par")
      parents = featureExtractorModel.transform(data)
    }

    // vector features
    var vectors = target

    if (tver) {
      featureExtractorModel.setMode("feat")
      vectors = featureExtractorModel.transform(data)
    }

    // information content
    var informationContent = parents

    if (res) {
      featureExtractorModel.setTarget(parents.select("parent").distinct()).setMode("info")
      informationContent = featureExtractorModel.transform(data)
    }

    // root distance
    var rootDist = parents

    if (wp) {
      featureExtractorModel.setTarget(parents.select("parent").distinct()).setMode("par2")
      val rooter = featureExtractorModel.transform(data)
      rootDist = rooter.groupBy("entity")
        .agg(max(rooter("depth")))
        .withColumnRenamed("max(depth)", "rootdist")
    }

    parents = parents.withColumnRenamed("entity", "entity_old")
    vectors = vectors.withColumnRenamed("entity", "entity_old")
    informationContent = informationContent.withColumnRenamed("entity", "entity_old")
    rootDist = rootDist.withColumnRenamed("entity", "entity_old")

    // join feature DataFrames
    val features = target
      .join(parents, target("entity") === parents("entity_old")).drop("entity_old")
      .join(vectors, target("entity") === vectors("entity_old")).drop("entity_old")
      .join(informationContent, target("entity") === informationContent("entity_old")).drop("entity_old")
      .join(rootDist, target("entity") === rootDist("entity_old")).drop("entity_old")


    // use models
    var result = target
    if (res) {
      val resnik = new ResnikModel()
      val temp = resnik.setTarget(target)
        .setDepth(iterationDepth)
        .setFeatures(features, "entity", "parent", "informationContent")
        .transform(data)
        .withColumnRenamed("entityA", "entityA_old")
        .withColumnRenamed("entityB", "entityB_old")
      result = result.join(temp, result("entityA") <=> temp("entityA_old") && result("entityB") <=> temp("entityB_old"))
    }
    if (wp) {

    }
    if (tver) {

    }
    result
  }

}
