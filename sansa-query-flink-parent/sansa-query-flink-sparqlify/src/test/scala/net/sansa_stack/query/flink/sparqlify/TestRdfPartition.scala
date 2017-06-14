package org.sansa_stack.query.flink.sparqlify

import net.sansa_stack.query.flink.sparqlify.SparqlifyUtils3
import net.sansa_stack.rdf.flink.partition.core.RdfPartitionUtilsFlink
import net.sansa_stack.rdf.partition.core.RdfPartitionDefault
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import org.apache.jena.graph.Triple
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.scalatest._

import scala.collection.JavaConverters._

class TestRdfPartition extends FlatSpec {

  "A partitioner" should "support custom datatypes" in {
    ExecutionEnvironment.getExecutionEnvironment.getConfig
    
    val env = ExecutionEnvironment.getExecutionEnvironment
    val flinkTable = TableEnvironment.getTableEnvironment(env)
    val triples = RDFDataMgr.createIteratorTriples(getClass.getResourceAsStream("/dbpedia-01.nt"), Lang.NTRIPLES, null).asScala
      //.map(t => RDFTriple(t.getSubject, t.getPredicate, t.getObject))
      .toList
    val dsAll: DataSet[Triple] = env.fromCollection(triples)
    val partition: Map[RdfPartitionDefault, DataSet[_ <: Product]] = RdfPartitionUtilsFlink.partitionGraph(dsAll)
    val views = SparqlifyUtils3.createSparqlSqlRewriter(env, flinkTable, partition)

    flinkTable.scan("deathPlace").printSchema();
    val res = flinkTable.sql(
      "SELECT s FROM deathPlace"
    )
    res.toDataSet[Row].print()
    //println(flinkTable.explain(res))
    //ds.print()
    //env.execute()
  }
}