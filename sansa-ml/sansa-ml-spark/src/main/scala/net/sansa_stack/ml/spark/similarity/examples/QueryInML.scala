package net.sansa_stack.ml.spark.similarity.examples

import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io._
import org.apache.spark.sql.SparkSession

import net.sansa_stack.query.spark.query._

object QueryInML {

  def main(args: Array[String]): Unit = {

    // setup spark session
    val spark = SparkSession.builder
      .appName(s"MinMal Semantic Similarity Estimation Calls")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val inputPath = "./sansa-ml/sansa-ml-spark/src/main/resources/movieData/movie.nt"

    val lang = Lang.NTRIPLES
    val graphRdd = spark.rdf(lang)(inputPath)

    val sparqlQuery = "SELECT * WHERE {?s ?p ?o} LIMIT 10"
    val result = graphRdd.sparql(sparqlQuery)
    result.rdd.foreach(println)
  }
}
