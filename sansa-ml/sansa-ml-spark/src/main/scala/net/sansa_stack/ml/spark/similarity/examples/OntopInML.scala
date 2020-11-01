package net.sansa_stack.ml.spark.similarity.examples

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import net.sansa_stack.query.spark.ontop.OntopSPARQLEngine
import net.sansa_stack.query.spark.query.OntopSPARQLExecutor
import net.sansa_stack.rdf.common.partition.core.RdfPartitionerComplex
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import org.apache.jena.graph.Node
import org.apache.jena.query.{Query, QueryFactory, ResultSet}
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.ResultSetStream
import org.apache.jena.sparql.engine.binding.Binding

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object OntopInML {

  private def resultSetFromBindings(query: Query, bindings: Array[Binding]): ResultSet = {
    val model = ModelFactory.createDefaultModel()
    val rs = new ResultSetStream(query.getResultVars, model, bindings.toList.asJava.iterator())
    rs
  }

  def main(args: Array[String]): Unit = {

    // setup spark session
    val spark = SparkSession.builder
      .appName(s"MinMal Semantic Similarity Estimation Calls")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val sparqlQuery = "SELECT * WHERE {?s ?p ?o} LIMIT 10"

    val inputPath = "./sansa-ml/sansa-ml-spark/src/main/resources/rdf.nt"

    // load an RDD of triples (from an N-Triples file here)
    val data = spark.rdf(Lang.NTRIPLES)(inputPath).cache()


    /* val query = QueryFactory.create(sparqlQuery)

    val sparqlExecutor = new OntopSPARQLExecutor(data)

    val result: Array[Binding] = sparqlExecutor.sparqlRDD(sparqlQuery).collect()

    val rs: ResultSet = resultSetFromBindings(query, result)

    rs.

     */



    // apply vertical partitioning which is necessary for the current Ontop integration
    val partitions = RdfPartitionUtilsSpark.partitionGraph(data, partitioner = RdfPartitionerComplex(false))

    // create the SPARQL engine
    val ontopEngine = OntopSPARQLEngine(spark, partitions, ontology = None)

    // run a SPARQL
    // a) SELECT query and return an RDD of bindings
    val result: RDD[Binding] = ontopEngine.execSelect(sparqlQuery).cache()

    result.foreach(println(_))

    println()
    val res: RDD[List[Any]] = result.map(b => {
      var vars: util.Iterator[Var] = b.vars()
      var tmpList = new ListBuffer[Any]()

      while (vars.hasNext) {
        val currentVar: Var = vars.next()
        var currentNode: Node = b.get(currentVar)
        if (currentNode.isLiteral) {
          val currentVal = currentNode.getLiteralValue
          println(currentNode, currentNode.getLiteralDatatype, currentVal)
          tmpList += currentVal
        }
        else {
          val currentVal = currentNode.getIndexingValue
          println(currentNode, currentNode.getURI, currentVal)
          tmpList += currentVal

        }
        // println(currentVar)
        // println(b.get(currentVar))
      }
      tmpList.toList
    }).cache()
    res.foreach(println(_))
    println()

    // def getColumnDataTypes()

  }
}

