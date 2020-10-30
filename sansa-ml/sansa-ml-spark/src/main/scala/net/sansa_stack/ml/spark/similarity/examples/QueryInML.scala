package net.sansa_stack.ml.spark.similarity.examples

import net.sansa_stack.query.spark.ontop.OntopSPARQLEngine
import org.apache.jena.riot.Lang
import org.apache.spark.sql.{Row, SparkSession}
import net.sansa_stack.query.spark.query._
import net.sansa_stack.rdf.common.partition.core.{RdfPartitionComplex, RdfPartitionerComplex}
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.rdd.RDD
import net.sansa_stack.ml.spark.featureExtractor.SparqlFeatureExtractorModel
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model._

object QueryInML {

  def main(args: Array[String]): Unit = {

    // setup spark session
    val spark = SparkSession.builder
      .appName(s"MinMal Semantic Similarity Estimation Calls")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // val inputPath = "./sansa-ml/sansa-ml-spark/src/main/resources/movieData/movie.nt"
    val inputPath = "./sansa-ml/sansa-ml-spark/src/main/resources/rdf.nt"
    val lmdbPath = "/Users/carstendraschner/sampleDataSets/lmdb/linkedmdb-latest-dump.nt"

    val ds = spark.read.rdf(Lang.NTRIPLES)(lmdbPath).toDS()

    val fe = new SparqlFeatureExtractorModel()
      .setQuery("SELECT ?o WHERE {?s ?p ?o} LIMIT 10")

    // fe.transform(ds)

    // assert(false)


    val lang = Lang.NTRIPLES
    val graphRdd = spark.rdf(lang)(inputPath)

    // with sparqlfy
    val sparqlQuery = "SELECT ?o WHERE {?s ?p ?o} LIMIT 10"

   /* val result1 = graphRdd.sparql(sparqlQuery)
    result1.rdd.foreach(println)

    for (res <- result1) {
      println(res)
    }

    */



    // with ontop
    // apply vertical partitioning which is necessary for the current Ontop integration
    val partitions: Map[RdfPartitionComplex, RDD[Row]] = RdfPartitionUtilsSpark.partitionGraph(graphRdd, partitioner = RdfPartitionerComplex(false))
    val ontopEngine = OntopSPARQLEngine(spark, partitions, ontology = None)
    val result2: RDD[Binding] = ontopEngine.execSelect(sparqlQuery)
    result2.foreach(println)
    for (b <- result2) {
      println(b.get((b.vars()).next()))
    }

    val tmpV = result2.collect()(0).vars().next()
    val tmp: RDD[String] = result2.map(_.get(tmpV).toString())
    tmp.foreach(println)
  }
}
