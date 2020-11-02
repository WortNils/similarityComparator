package net.sansa_stack.ml.spark.similarity.examples

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import net.sansa_stack.query.spark.ontop.OntopSPARQLEngine
import net.sansa_stack.query.spark.query.OntopSPARQLExecutor
import net.sansa_stack.rdf.common.partition.core.RdfPartitionerComplex
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import org.apache.jena.graph
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

    val input = "./sansa-ml/sansa-ml-spark/src/main/resources/rdf.nt"

    val input1 = "/Users/carstendraschner/sciebo-research/PLATOON/ENGIE-UBO-accident-use-case/TeamsData/TTL_files_of_CSV/Traffic_Accident_Injury_Database_2018/caracteristiques_2018_out_1.ttl"
    val sparqlQuery1 = """SELECT DISTINCT ?accidentId ?long ?lat ?datetime ?weatherCondition ?lightingCondition ?colisionCondition
                         |WHERE {
                         |
                         |?accidentId <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.engie.fr/ontologies/accidentontology/RoadAccident> .
                         |
                         |?accidentId <https://w3id.org/seas/location> ?location .
                         |?location <http://www.w3.org/2003/01/geo/wgs84_pos#point> ?point .
                         |?point <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?long .
                         |?point <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?lat .
                         |
                         |?accidentId <https://w3id.org/seas/hasTemporalContext> ?tempInfo .
                         |?tempInfo <http://www.w3.org/2006/time#inXSDDateTime> ?datetime .
                         |
                         |?accidentId <http://www.engie.fr/ontologies/accidentontology/hasEnvironmentDescription> ?environmentDescription .
                         |?environmentDescription <http://www.engie.fr/ontologies/accidentontology/colisionCondition> ?colisionCondition .
                         |?environmentDescription <http://www.engie.fr/ontologies/accidentontology/weatherCondition> ?weatherCondition .
                         |?environmentDescription <http://www.engie.fr/ontologies/accidentontology/lightingCondition> ?lightingCondition .
                         |}""".stripMargin

    // load an RDD of triples (from an N-Triples file here)
    val data: RDD[graph.Triple] = spark.rdf(Lang.TURTLE)(input).cache()
    // data.foreach(print(_))

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

