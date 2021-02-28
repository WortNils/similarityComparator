package net.sansa_stack.ml.spark.evaluation.utils

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import collection.JavaConverters._

class PairCreator {
  val spark = SparkSession.builder.getOrCreate()

  def create (dataset: Dataset[_]): DataFrame = {
    import spark.implicits._

    val ds: Dataset[(String, String, String)] = dataset.as[(String, String, String)]

    val temper = ds.flatMap(t => Seq((t._1), (t._3))).collectAsList()
    val temp = temper.asScala
    val l = temp.length
    val arr = new Array[(String, String)](l * l)
    // var i: Int = 0
    for (i <- 0 until (l-1)) {
      // var j: Int = 0
      for (j <- 0 until (l-1)) {
        arr(i * l + j) = (temp(i), temp(j))
      }
    }
    val ret = arr.toSeq
    val rdd = spark.sparkContext.parallelize(ret)
    val retDf = rdd.toDF("entityA", "entityB")
    retDf
    }
}
