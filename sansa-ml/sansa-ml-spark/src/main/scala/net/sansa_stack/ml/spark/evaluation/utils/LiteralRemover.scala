package net.sansa_stack.ml.spark.evaluation.utils

import org.apache.jena.graph.Triple
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

class LiteralRemover extends Transformer {
  val spark = SparkSession.builder.getOrCreate()
  import spark.implicits._
  private val _availableModes = Array("none", "http", "bool")
  private var _mode = "none"

  /**
   * This method changes how literals are removed
   * @param mode a string specifying the mode
   * @return returns the FeatureExtractor
   */
  def setMode(mode: String): this.type = {
    if (_availableModes.contains(mode)) {
      _mode = mode
      this
    }
    else {
      throw new Exception("The specified mode: " + mode + " is not supported. Currently available are: " + _availableModes)
    }
  }

  def transform (dataset: Dataset[_]): DataFrame = {

    /*
    implicit val tripleEncoder = Encoders.kryo(classOf[Triple])
    val data: Dataset[Triple] = dataset.as[Triple]
    */

    val ds: DataFrame = dataset.toDF()

    val raw: DataFrame = _mode match {
      case "none" =>
        ds
      case "http" =>
        ds.where(ds("s").contains("http://") && ds("p").contains("http://") && ds("o").contains("http://"))
      case "bool" =>
        // data.filter(_.getObject.isURI()).toDF()
        ds
    }
    raw
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  override val uid: String = "FIXME"
}
