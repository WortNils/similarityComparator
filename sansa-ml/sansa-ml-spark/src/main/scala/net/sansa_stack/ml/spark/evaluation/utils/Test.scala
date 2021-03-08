package net.sansa_stack.ml.spark.evaluation.utils

import org.apache.jena.sys.JenaSystem
import org.apache.spark.sql.SparkSession

import scala.collection.Map

object Test {
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
    val thing: Map[String, Double] = Map("ab" -> 2)
    val blop = Seq("aa", "ab")
    val blip = blop.map(thing(_)).toDF
    blip.show()
  }
}
