package net.sansa_stack.ml.spark.evaluation.utils

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class SimilaritySampler extends Transformer {
  val spark = SparkSession.builder.getOrCreate()
  private val _availableModes = Array("rand", "cross")
  private var _mode: String = "cross"
  private val _outputCols: Array[String] = Array("entityA", "entityB")

  def setMode(mode: String): this.type = {
    if (_availableModes.contains(mode)) {
      _mode = mode
      this
    }
    else {
      throw new Exception("The specified mode: " + mode + "is not supported. Currently available are: " + _availableModes)
    }
  }

  def transform(dataset: Dataset[_]): DataFrame = {
    import spark.implicits._

    val ds: Dataset[(String, String, String)] = dataset.as[(String, String, String)]

    val raw = ds.flatMap(t => Seq((t._1), (t._3))).distinct().toDF()

    val rawDF: DataFrame = _mode match {
      case "cross" =>
        raw.crossJoin(raw)
      case "rand" =>
        raw.crossJoin(raw)
    }

    val cols = raw.columns.toSeq

    val retDf = rawDF
      .withColumnRenamed(cols(0), _outputCols(0))
      .withColumnRenamed(cols(1), _outputCols(1))
      .distinct()
    retDf
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  override val uid: String = "FIXME"
}
