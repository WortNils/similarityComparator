package net.sansa_stack.ml.spark.evaluation

import com.mysql.cj.exceptions.WrongArgumentException
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

    if (models.contains("Resnik")) {
      res = true
    }
    if (models.contains("WuAndPalmer")) {
      wp = true
    }
    if (models.contains("Tversky")) {
      tver = true
    }

    val sampler = new SimilaritySampler()
    val target: DataFrame = sampler
      .setMode(_samplingMode)
      .setLimit(_samplingLimit)
      .setSeed(_samplingSeed)
      .setLiteralRemoval(_literalRemovalMode)
      .transform(data)

    val featureExtractorModel = new FeatureExtractorEval().setDepth(iterationDepth)
    var extractionModes: ArrayBuffer[String] = ArrayBuffer("")

    val realTarget = target.drop("entityA")
      .withColumnRenamed("entityB", "uri")
      .union(target.drop("entityB")
        .withColumnRenamed("entityA", "uri"))
      .distinct()

    // feature Extraction
    // parents
    var parents = spark.emptyDataFrame

    if (wp) {
      featureExtractorModel.setMode(WP_mode)
      parents = featureExtractorModel.transform(data)
    }
    if (res && (!wp || WP_mode != "join")) {
      featureExtractorModel.setMode("par")
      parents = featureExtractorModel.transform(data)
    }

    if (res) {

    }

    if (tver) {

    }




    // information content
    // root distance
    // vector features
    if (res && wp && WP_mode == "join") {
      var feat = spark.emptyDataFrame
      if (tver) {
        featureExtractorModel.setMode("feat")
        feat = featureExtractorModel
          .transform(data)
      }
      val parents = featureExtractorModel
        .transform(data)

      val bparents: DataFrame = parents
        .withColumn("parent2", toTuple(col("parent"), col("depth")))
        .drop("parent", "depth")
        .groupBy("entity")
        .agg(collect_list("parent2"))
        .withColumnRenamed("collect_list(parent2)", "parents")

      val features: DataFrame = target.join(bparents, target("entityA") === bparents("entity"))
        .drop("entity")
        .withColumnRenamed("parents", "featuresA")
        .join(bparents, target("entityB") === bparents("entity"))
        .drop("entity")
        .withColumnRenamed("parents", "featuresB")

      val rents = parents.drop("entity", "depth").distinct().withColumnRenamed("parent", "uri")
      val rooter = featureExtractorModel.setMode("par2").setTarget(rents).transform(data)
      val roots = rooter.groupBy("entity")
        .agg(max(rooter("depth")))
        .withColumnRenamed("max(depth)", "rootdist")


      // TODO: think about how to input features properly
      val featureFrame = target.join(features, target("entityA") === features("uri")).drop("uri")
        .withColumnRenamed("vectorizedFeatures", "featuresA")
        .join(features, target("entityB") === features("uri")).drop("uri")
        .withColumnRenamed("vectorizedFeatures", "featuresB")
    } else {
      if (res) {
        extractionModes.append("par")
      }
      if (wp) {
        extractionModes.append(WP_mode)
      }
      if (tver) {
        extractionModes.append("feat")
      }
    }

    var extractedFeatures = spark.emptyDataFrame

    for (mode <- extractionModes) {
      val tempFeatures = featureExtractorModel.setMode(mode).transform(target)
      // extractedFeatures = extractedFeatures.union()
    }

    target
  }

}
