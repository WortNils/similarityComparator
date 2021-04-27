package net.sansa_stack.ml.spark.evaluation.utils

import org.apache.jena.graph.Triple
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

class LiteralRemover extends Transformer {
  val spark = SparkSession.builder.getOrCreate()
  import spark.implicits._
  private val _availableLiteralRemoval = Array("none", "http")
  private var _litMode = "none"

  def transform (dataset: Dataset[_]): DataFrame = {

    implicit val tripleEncoder = Encoders.kryo(classOf[Triple])
    val data: Dataset[Triple] = dataset.as[Triple]

    val ds: DataFrame = dataset.toDF()

    val raw: DataFrame = _litMode match {
      case "none" =>
        ds
      case "http" =>
        ds.where(ds("_1").contains("http://"))
      case "bool" =>
        data.filter(_.getObject.isURI()).toDF()
    }
    raw
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  override val uid: String = "FIXME"
}
