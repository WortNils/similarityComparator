package net.sansa_stack.ml.spark.featureExtractor

import org.apache.jena.graph.{Node, Triple}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import net.sansa_stack.query.spark.query._
import net.sansa_stack.query.spark.ontop.OntopSPARQLEngine
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io._
import org.apache.spark.sql.{Row, SparkSession}
import net.sansa_stack.query.spark.query._
import net.sansa_stack.rdf.common.partition.core.{RdfPartitionComplex, RdfPartitionerComplex}
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.rdd.RDD


/**
 * This class creates from a dataset of triples a feature representing dataframe which is needed for steps like spark mllib countVect
 */
class SparqlFeatureExtractorModel /* extends Transformer */ {
  val spark = SparkSession.builder.getOrCreate()
  private var _query: String = _
  private var _outputCol: String = "extractedFeatures"

  def setOutputCol(colName: String): this.type = {
    _outputCol = colName
    this
  }

  def setQuery(query: String): this.type = {
    _query = query
    this
  }

  /**
   * takes read in dataframe and produces a dataframe with features
   * @param dataset most likely a dataframe read in over sansa rdf layer
   * @return a dataframe with two columns, one for string of URI and one of a list of string based features
   */
  def transform(dataset: Dataset[Triple]): Unit = {

    val graphRdd = dataset.rdd // .asInstanceOf[RDD[Triple]]

    // with sparqlfy
    val sparqlQuery = "SELECT ?o WHERE {?s ?p ?o} LIMIT 10"
    val result1 = graphRdd.sparql(sparqlQuery)

    result1.show(false)

    // val tmp: List = result1

    for (res <- result1) {
      println(res)
    }

  }

  /* override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  override val uid: String = "FIXME" */
}
