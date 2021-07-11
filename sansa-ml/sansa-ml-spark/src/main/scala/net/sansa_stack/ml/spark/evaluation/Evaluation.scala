package net.sansa_stack.ml.spark.evaluation

import net.sansa_stack.ml.spark.evaluation.models._
import net.sansa_stack.ml.spark.evaluation.utils._
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.sql.{DataFrame, SparkSession}

object Evaluation {
  def main(args: Array[String]): Unit = {
    // setup spark session
    val spark: SparkSession = SparkSession.builder
      .appName(s"Semantic Similarity Evaluator")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // cause of jena NPE issue
    JenaSystem.init()

    import spark.implicits._

    // define inputpath if it is not parameter
    val inputPath = "./sansa-ml/sansa-ml-spark/src/main/resources/movieData/movie.ttl"
    val outputPath = "C://evaluationData/movie3.csv"
    // val inputPath2 = "D:/Benutzer/Nils/sciebo/Bachelorarbeit/Datasets/linkedmdb-18-05-2009-dump.nt"
    val inputPath2 = "D:/Benutzer/Nils/sciebo/Bachelorarbeit/Datasets/wordnet.nt"

    // read in data as Data`Frame
    // val triplesrdd = spark.rdf(Lang.NTRIPLES)(inputPath).cache()
    // val triplesDF: DataFrame = spark.rdf(Lang.NTRIPLES)(inputPath).toDF().cache()

    /*
    val triplesDS = NTripleReader
      .load(
        spark,
        inputPath2,
        stopOnBadTerm = ErrorParseMode.SKIP,
        stopOnWarnings = WarningParseMode.IGNORE)
      .toDS().cache()
     */

    /*
    val triplesDF = NTripleReader
      .load(
        spark,
        inputPath2,
        stopOnBadTerm = ErrorParseMode.SKIP,
        stopOnWarnings = WarningParseMode.IGNORE)
      .toDF().cache()
    */

    val triplesDF = spark.rdf(Lang.TTL)(inputPath).toDF().cache()

    /*
    val _queryString = "SELECT ?s ?p ?o WHERE {?s ?p ?o}"
    val sparqlFrame = new SparqlFrame()
      .setSparqlQuery(_queryString)
    val res = sparqlFrame.transform(triplesDF)
    res.show(false)
     */

    triplesDF.show(false)
    println("#triples: " + triplesDF.count())

    // set input uris
    // val target: DataFrame = Seq(("<m1>", "<m2>"), ("<m2>", "<m1>")).toDF()
    /* val target: DataFrame = Seq(("file:///C:/Users/nilsw/IdeaProjects/similarityComparator/m3", "file:///C:/Users/nilsw/IdeaProjects/similarityComparator/m2")).toDF()
      .withColumnRenamed("_1", "entityA").withColumnRenamed("_2", "entityB") */
    val sampler = new SimilaritySampler()
    val target: DataFrame = sampler.setMode("limit")
      .setLimit(10)
      .setLiteralRemoval("none")
      .transform(triplesDF)
    // val target: DataFrame = sampler.setMode("cross").transform(triplesDF)

    target.show(false)


    /*
    val resnik = new ResnikModel()
    val result = resnik.setTarget(target)
      .setDepth(5)
      .transform(triplesDF)
    result.show(false)


    val featureExtractor = new FeatureExtractorEval()
      .setMode("par").setDepth(5)
      .setTarget(target.drop("entityA")
        .withColumnRenamed("entityB", "uri")
        .union(target.drop("entityB")
          .withColumnRenamed("entityA", "uri"))
        .distinct())
    val parents = featureExtractor.transform(triplesDF)

    val informationC = featureExtractor.setMode("ic")
      .transform(triplesDF)
      .withColumnRenamed("entity", "entity2")

    val features = parents.join(informationC, parents("parent") === informationC("entity2")).drop("entity2")
    features.show(false)

    val resnik2 = new ResnikModel()
    val result_2 = resnik2.setTarget(target)
      .setFeatures(features, "entity", "parent", "informationContent")
      .transform(target)

    result_2.show(false) */

    /*
    val wuandpalmer = new WuAndPalmerModel()
    val result2 = wuandpalmer.setTarget(target)
      .setDepth(5).setMode("breadth")
      .transform(triplesDF)
    result2.show(false) */

    val t0 = System.currentTimeMillis()

    val wuandpalmer = new WuAndPalmerModel().setTarget(target)
      .setDepth(5).setMode("join")

    val t1 = System.currentTimeMillis()

    val result2 = wuandpalmer
      .transform(triplesDF)

    val t2 = System.currentTimeMillis()

    result2.show(false)

    val t3 = System.currentTimeMillis()

    println("time manual: ")
    println((t0-t1)/1000)
    println((t0-t2)/1000)
    println((t0-t3)/1000)

    /*
    val tversky = new TverskyModel()
    val result3 = tversky.setTarget(target)
      .transform(triplesDF)
    result3.show(false)
    */


    /*
    result.filter((result("entityA") === "file:///C:/Users/nilsw/IdeaProjects/similarityComparator/p1" && result("entityB") === "file:///C:/Users/nilsw/IdeaProjects/similarityComparator/p2") || result("entityB") === "file:///C:/Users/nilsw/IdeaProjects/similarityComparator/p1" && result("entityA") === "file:///C:/Users/nilsw/IdeaProjects/similarityComparator/p2").show(false)

    result2.filter((result2("entityA") === "file:///C:/Users/nilsw/IdeaProjects/similarityComparator/p1" && result2("entityB") === "file:///C:/Users/nilsw/IdeaProjects/similarityComparator/p2") || result2("entityB") === "file:///C:/Users/nilsw/IdeaProjects/similarityComparator/p1" && result2("entityA") === "file:///C:/Users/nilsw/IdeaProjects/similarityComparator/p2").show(false)

    result4.filter((result4("entityA") === "file:///C:/Users/nilsw/IdeaProjects/similarityComparator/p1" && result4("entityB") === "file:///C:/Users/nilsw/IdeaProjects/similarityComparator/p2") || result4("entityB") === "file:///C:/Users/nilsw/IdeaProjects/similarityComparator/p1" && result4("entityA") === "file:///C:/Users/nilsw/IdeaProjects/similarityComparator/p2").show(false)

    result3.filter((result3("entityA") === "file:///C:/Users/nilsw/IdeaProjects/similarityComparator/p1" && result3("entityB") === "file:///C:/Users/nilsw/IdeaProjects/similarityComparator/p2") || result3("entityB") === "file:///C:/Users/nilsw/IdeaProjects/similarityComparator/p1" && result3("entityA") === "file:///C:/Users/nilsw/IdeaProjects/similarityComparator/p2").show(false)
    */

    /*
    val asGraph = new SimilarityExperimentMetaGraphFactory()
    val ResGraph = asGraph.createRdfOutput(
      result
    )(
      modelInformationEstimatorName = "Resnik",
      modelInformationEstimatorType = "SimilarityEstimation",
      modelInformationMeasurementType = "Similarity"
    )(
      inputDatasetNumbertOfTriples = triplesDF.count(),
      dataSetInformationFilePath = inputPath)
    ResGraph.take(10).foreach(println(_)) */

    // ResGraph.coalesce(1).saveAsNTriplesFile("./sansa-ml/sansa-ml-spark/src/main/resources/movieData/movieResult")

    // show results

    /*
    val finaldf = result.join(result2, Seq("entityA", "entityB"))
    finaldf.show(false)
    finaldf.coalesce(1).write.mode("overwrite").csv(outputPath)

     */


  /*
    val sampleUri: String = "http://wordnet-rdf.princeton.edu/id/01383647-a"
    // feature extraction
    val featureExtractorModel = new FeatureExtractorModel()
      .setMode("an")
    val extractedFeaturesDataFrame = featureExtractorModel
      .transform(triplesDF)
      .filter(t => t.getAs[String]("uri").startsWith(sampleUri))
      .cache()
    extractedFeaturesDataFrame.show(false)

    // filter for relevant URIs e.g. only movies
    val filteredFeaturesDataFrame = extractedFeaturesDataFrame.filter(t => t.getAs[String]("uri").startsWith(sampleUri)).cache()
    filteredFeaturesDataFrame.show(false)

    // count Vectorization
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("extractedFeatures")
      .setOutputCol("vectorizedFeatures")
      .fit(filteredFeaturesDataFrame)
    val tmpCvDf: DataFrame = cvModel.transform(filteredFeaturesDataFrame)
    // val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 }, DataTypes.BooleanType)
    val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 })
    val countVectorizedFeaturesDataFrame: DataFrame = tmpCvDf.filter(isNoneZeroVector(col("vectorizedFeatures"))).select("uri", "vectorizedFeatures").cache()
    countVectorizedFeaturesDataFrame.show(false)

    // similarity Estimations Overview
    // for nearestNeighbors we need one key which is a Vector to search for NN
    // val sample_key: Vector = countVectorizedFeaturesDataFrame.take(1)(0).getAs[Vector]("vectorizedFeatures")
    val sample_key: Vector = countVectorizedFeaturesDataFrame
      .filter(countVectorizedFeaturesDataFrame("uri") === sampleUri)
      .take(1)(0)
      .getAs[Vector]("vectorizedFeatures")

    val tverskyModel = new TverskyModel().setInputCol("vectorizedFeatures")
      .setAlpha(1.0)
      .setBeta(1.0)
    tverskyModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10, keyUri = sampleUri).show()
    tverskyModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).show() */
  }
}
