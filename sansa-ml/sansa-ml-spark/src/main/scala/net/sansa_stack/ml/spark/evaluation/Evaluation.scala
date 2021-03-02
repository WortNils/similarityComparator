package net.sansa_stack.ml.spark.evaluation

import net.sansa_stack.ml.spark.evaluation.models._
import net.sansa_stack.ml.spark.evaluation.utils._
import net.sansa_stack.ml.spark.utils.SimilarityExperimentMetaGraphFactory
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.graph._
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

object Evaluation {
  def main(args: Array[String]): Unit = {
    // setup spark session
    val spark = SparkSession.builder
      .appName(s"Semantic Similarity Evaluator")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // cause of jena NPE issue
    JenaSystem.init()

    import spark.implicits._

    // define inputpath if it is not parameter
    val inputPath = "./sansa-ml/sansa-ml-spark/src/main/resources/movieData/movie.nt"
    // val inputPath = "C:/Users/nilsw/sciebo/Bachelorarbeit/Datasets/linkedmdb-18-05-2009-dump.nt"

    // read in data as Data`Frame
    /* val triplesrdd = spark.rdf(Lang.NTRIPLES)(inputPath).cache()

    triplesrdd.foreach(println(_))
    triplesrdd.toDF().show(false) */

    val triplesDF: DataFrame = spark.rdf(Lang.NTRIPLES)(inputPath).toDF().cache() // Seq(("<a1>", "<ai>", "<m1>"), ("<m1>", "<pb>", "<p1>")).toDF()
    /* val triplesRDD: RDD[org.apache.jena.graph.Triple] = NTripleReader.load(
      spark,
      inputPath,
      stopOnBadTerm = ErrorParseMode.SKIP,
      stopOnWarnings = WarningParseMode.IGNORE
    ).cache() // .cache()

    // triplesDS.take(10).foreach(println(_))

    implicit val tripleEncoder = Encoders.kryo(classOf[Triple])

    val triplesDF = triplesRDD.as[Triple].toDF()
*/
    triplesDF.show(false)

    // set input uris
    // val target: DataFrame = Seq(("<m1>", "<m2>"), ("<m2>", "<m1>")).toDF()
    /* val target: DataFrame = Seq(("file:///C:/Users/nilsw/IdeaProjects/similarityComparator/m3", "file:///C:/Users/nilsw/IdeaProjects/similarityComparator/m2")).toDF()
      .withColumnRenamed("_1", "entityA").withColumnRenamed("_2", "entityB") */
    val pairizer = new PairCreator()
    val target: DataFrame = pairizer.create(triplesDF)
    // val target: DataFrame = triplesDF.crossJoin(triplesDF)

    target.show(false)

    /*
    val featureExtractorModel = new FeatureExtractorEval()
      .setMode("ic")
    val info = featureExtractorModel
      .transform(triplesDF)

    info.show(false)

    val par = featureExtractorModel.setMode("par").transform(triplesDF)

    par.show(false)
    */

    val resnik = new ResnikModel()
    val result = resnik.setTarget(target).setDepth(5).transform(triplesDF)
    result.show(false)

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
    ResGraph.take(10).foreach(println(_))
    // ResGraph.coalesce(1).saveAsNTriplesFile("./sansa-ml/sansa-ml-spark/src/main/resources/movieData/movieResult")

    // similarity measures
    /* val similarityMeasures = ["Resnik", "Wu and Palmer", "Tversky", "Knappe"]
    for (var sim <- similarityMeasures) {
      // final = target.join(sim.transform(triplesDF, target))
    }
    */
    // show results
  }
}
