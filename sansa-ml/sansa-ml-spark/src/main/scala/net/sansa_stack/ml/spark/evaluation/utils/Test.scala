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
    /*
    val thing: Map[String, Double] = Map("ab" -> 2)
    val blop = Seq("aa", "ab")
    val blip = blop.map(thing(_)).toDF
    blip.show()
    */
    val a: List[(String, Int)] = List(("a", 1), ("b", 2))
    val b: List[(String, Int)] = List(("a", 3), ("b", 1), ("c", 5))

    val a2 = a.toMap
    val b2 = b.toMap

    // val c = a.union(b).groupBy(_._1).mapValues(seq => seq.reduce{(x, y) => (x._1, x._2 + y._2)}._2).toList
    val c2 = a2.keySet.intersect(b2.keySet).map(key => (key, a2(key) + b2(key)))
    val f = c2.minBy(_._2)
    println(c2)
    println(f)
  }
}
