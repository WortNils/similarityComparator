package net.sansa_stack.ml.spark.evaluation

import net.sansa_stack.ml.spark.evaluation.models.{ResnikModel, TverskyModel, WuAndPalmerModel}
import net.sansa_stack.ml.spark.evaluation.utils.{FeatureExtractorEval, SimilaritySampler}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.types.StructType


class SimilarityWrapper extends Transformer {
  val spark: SparkSession = SparkSession.builder.getOrCreate()

  // wrapper variables
  private var res: Boolean = false
  private var wp: Boolean = false
  private var tver: Boolean = false

  // model hyperparameters
  private var _iterationDepth: Int = 5
  private val _availableWPModes = Array("join", "breadth", "path")
  private var _wpMode: String = "join"
  private var _tverAlpha: Double = 1.0
  private var _tverBeta: Double = 1.0

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
   * @param samplingLimit an Int specifying the amount of rows to take
   * @param samplingSeed a Long specifying the random seed
   * @param samplingQuery a String specifying the sparql query
   * @return returns Wrapper
   */
  def setSamplingMode(samplingMode: String,
                      samplingLimit: Int = 10,
                      samplingSeed: Long = 20,
                      samplingQuery: String = "SELECT ?s ?p ?o WHERE {?s ?p ?o}"): this.type = {
    if (_availableSamplingModes.contains(samplingMode)) {
      _samplingMode = samplingMode
    }
    else {
      throw new IllegalArgumentException("The specified mode: " + samplingMode + " is not supported. " +
        "Currently available are: " + _availableSamplingModes.mkString(", "))
    }
    if (samplingLimit > 0) {
      _samplingLimit = samplingLimit
    } else {
      throw new IllegalArgumentException("The specified limit was negative.")
    }
    if (samplingSeed > 0) {
      _samplingSeed = samplingSeed
    } else {
      throw new IllegalArgumentException("The specified seed was negative.")
    }
    // TODO: Add check for valid sparql query
    _samplingSparql = samplingQuery
    this
  }

  /**
   * This method changes the amount of rows to take in limit and rand sampling mode
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
   * @param samplingSparql a String specifying the sparql query
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


  // model setters
  /**
   * This method lets you set all available models and their hyperparameters.
   * @param resnik Boolean value for the resnik model, default value false
   * @param wuandpalmer Boolean value for the wuandpalmer model, default value false
   * @param tversky Boolean value for the tversky model, default value false
   * @param iterationDepth Integer value for the iterationdepth, default value 5
   * @param WP_mode String value for the wuandpalmer mode, default value "join"
   * @param T_alpha Double value for the tversky alpha value, default value 1.0
   * @param T_beta Double value for the tversky beta value, default value 1.0
   * @return the wrapper
   */
  def setModels(resnik: Boolean = false,
                wuandpalmer: Boolean = false,
                tversky: Boolean = false,
                iterationDepth: Int = 5,
                WP_mode: String = "join",
                T_alpha: Double = 1.0,
                T_beta: Double = 1.0): this.type = {
    if (resnik) res = resnik
    if (wuandpalmer) wp = wuandpalmer
    if (tversky) tver = tversky
    if (iterationDepth > 0) {
      _iterationDepth = iterationDepth
    } else {
      throw new IllegalArgumentException("Iteration depth must be a positive Integer.")
    }
    if (_availableWPModes.contains(WP_mode)) {
      _wpMode = WP_mode
    } else {
      throw new IllegalArgumentException("The specified Wu and Palmer mode: " + WP_mode + " is not supported. " +
        "Currently available are: " + _availableWPModes.mkString(", "))
    }
    if (T_alpha >= 0.0 && T_alpha <= 1.0) {
      _tverAlpha = T_alpha
    }
    else {
      throw new IllegalArgumentException("T_alpha must be between 0 and 1.")
    }
    if (T_beta >= 0.0 && T_beta <= 1.0) {
      _tverBeta = T_beta
    }
    else {
      throw new IllegalArgumentException("T_beta must be between 0 and 1.")
    }
    this
  }

  /**
   * This method lets you turn the resnik model on or off
   * @param resnik Boolean
   * @return returns the Wrapper
   */
  def setResnik(resnik: Boolean): this.type = {
    res = resnik
    this
  }

  /**
   * This method lets you turn the wuandpalmer model on or off
   * @param wuandpalmer Boolean
   * @return returns the Wrapper
   */
  def setWuAndPalmer(wuandpalmer: Boolean): this.type = {
    wp = wuandpalmer
    this
  }

  /**
   * This method lets you turn the tversky model on or off
   * @param tversky Boolean
   * @return returns the Wrapper
   */
  def setTversky(tversky: Boolean): this.type = {
    tver = tversky
    this
  }

  /**
   * This method lets you decide the iterationDepth aka maximum depth of parent search
   * @param depth Integer
   * @return returns the Wrapper
   */
  def setIterationDepth(depth: Int): this.type = {
    if (depth > 0) {
      _iterationDepth = depth
      this
    } else {
      throw new IllegalArgumentException("The specified amount was negative.")
    }
  }

  /**
   * This method changes what algorithm is used for calculating the parents for Wu And Palmer
   * @param WP_mode a string specifying the mode
   * @return returns the Wrapper
   */
  def setWPMode(WP_mode: String): this.type = {
    if (_availableWPModes.contains(WP_mode)) {
      _wpMode = WP_mode
      this
    } else {
      throw new IllegalArgumentException("The specified Wu and Palmer mode: " + WP_mode + " is not supported. " +
        "Currently available are: " + _availableWPModes.mkString(", "))
    }
  }

  /**
   * This method changes the alpha parameter of the tversky model
   * @param T_alpha Double
   * @return returns the Wrapper
   */
  def setTalpha(T_alpha: Double): this.type = {
    if (T_alpha >= 0.0 && T_alpha <= 1.0) {
      _tverAlpha = T_alpha
      this
    }
    else {
      throw new IllegalArgumentException("T_alpha must be between 0 and 1.")
    }
  }

  /**
   * This method changes the beta parameter of the tversky model
   * @param T_beta Double
   * @return returns the Wrapper
   */
  def setTbeta(T_beta: Double): this.type = {
    if (T_beta >= 0.0 && T_beta <= 1.0) {
      _tverBeta = T_beta
      this
    }
    else {
      throw new IllegalArgumentException("T_beta must be between 0 and 1.")
    }
  }

  /**
   * Takes read in dataframe, and target dataframe and produces a dataframe with similarity values for the specified models
   * @param dataset a dataframe read in over sansa rdf layer
   * @return a dataframe with two columns for the entities, one for the similarity value and one for the time for
   *         each similarity model, so at most eight columns
   */
  override def transform (dataset: Dataset[_]): DataFrame = {
    // catching wrong arguments
    if (dataset.isEmpty) {
      throw new IllegalArgumentException("The target DataFrame is empty.")
    }

    // sampling
    val sampler = new SimilaritySampler()
    val target: DataFrame = sampler
      .setMode(_samplingMode)
      .setLimit(_samplingLimit)
      .setSeed(_samplingSeed)
      .setLiteralRemoval(_literalRemovalMode)
      .transform(dataset)


    // feature Extraction
    val featureExtractorModel = new FeatureExtractorEval().setDepth(_iterationDepth)

    val realTarget = target.drop("entityA")
      .withColumnRenamed("entityB", "uri")
      .union(target.drop("entityB")
        .withColumnRenamed("entityA", "uri"))
      .distinct()

    featureExtractorModel.setTarget(realTarget)

    // parents
    var parents = realTarget

    if (wp && _wpMode == "join") {
      featureExtractorModel.setMode("par2")
      parents = featureExtractorModel.transform(dataset)
    }
    if (res && (!wp || _wpMode != "join")) {
      featureExtractorModel.setMode("par")
      parents = featureExtractorModel.transform(dataset)
    }

    // vector features
    var vectors = realTarget

    if (tver) {
      featureExtractorModel.setMode("feat")
      vectors = featureExtractorModel.transform(dataset)
    }

    // information content
    var informationContent = parents

    if (res) {
      featureExtractorModel.setTarget(parents.select("parent").distinct()).setMode("info")
      informationContent = featureExtractorModel.transform(dataset)
    }

    // root distance
    var rootDist = parents

    if (wp) {
      featureExtractorModel.setTarget(parents.select("parent").distinct()).setMode("par2")
      val rooter = featureExtractorModel.transform(dataset)
      rootDist = rooter.groupBy("entity")
        .agg(max(rooter("depth")))
        .withColumnRenamed("max(depth)", "rootdist")
    }

    // join feature DataFrames
    val features = realTarget
      .join(parents, realTarget("uri") === parents("entity")).drop("entity")
      .join(vectors, realTarget("uri") === vectors("entity")).drop("entity")
      .join(informationContent, realTarget("uri") === informationContent("entity")).drop("entity")
      .join(rootDist, realTarget("uri") === rootDist("entity")).drop("entity")


    // use models
    var result = target
    if (res) {
      val resnik = new ResnikModel()
      val temp = resnik.setTarget(target)
        .setDepth(_iterationDepth)
        .setFeatures(features, "entity", "parent", "informationContent")
        .transform(dataset)
        .withColumnRenamed("entityA", "entityA_old")
        .withColumnRenamed("entityB", "entityB_old")
      result = result.join(temp, result("entityA") <=> temp("entityA_old") && result("entityB") <=> temp("entityB_old"))
        .drop("entityA_old").drop("entityB_old")
    }
    if (wp) {
      val wupalm = new WuAndPalmerModel()
      val temp = wupalm.setTarget(target)
        .setDepth(_iterationDepth)
        .setFeatures(features, "entity", "parent", "depth", "rootDist")
        .transform(dataset)
        .withColumnRenamed("entityA", "entityA_old")
        .withColumnRenamed("entityB", "entityB_old")
      result = result.join(temp, result("entityA") <=> temp("entityA_old") && result("entityB") <=> temp("entityB_old"))
        .drop("entityA_old").drop("entityB_old")
    }
    if (tver) {
      val tversky = new TverskyModel()
      val temp = tversky.setTarget(target)
        .setDepth(_iterationDepth)
        .setFeatures(features, "entity", "vectorizedFeatures")
        .transform(dataset)
        .withColumnRenamed("entityA", "entityA_old")
        .withColumnRenamed("entityB", "entityB_old")
      result = result.join(temp, result("entityA") <=> temp("entityA_old") && result("entityB") <=> temp("entityB_old"))
        .drop("entityA_old").drop("entityB_old")
    }
    result
  }
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  override val uid: String = "FIXME"
}
