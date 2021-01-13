package net.sansa_stack.ml.spark.evaluation

import net.sansa_stack.ml.spark.evaluation.models._
import net.sansa_stack.ml.spark.evaluation.utils._
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Evaluation {
  def main(args: Array[String]): Unit = {
    // setup spark session
    val spark = SparkSession.builder
      .appName(s"MinMal Semantic Similarity Estimation Calls")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // cause of jena NPE issue TODO ask Claus what is solution
    JenaSystem.init()

    // define inputpath if it is not parameter
    val inputPath = "./sansa-ml/sansa-ml-spark/src/main/resources/movieData/movie.nt"

    // read in data as Data`Frame
    val triplesDf: DataFrame = spark.read.rdf(Lang.NTRIPLES)(inputPath).cache()

    triplesDf.show(false)

    // set input uris

    // similarity measures
    val similarityMeasures = ["Resnik", "Wu and Palmer", "Tversky", "Knappe"]
    for (var sim <- similarityMeasures) {
      // sim.transform()
    }

    // show results

  }
}
