package net.sansa_stack.rdf.flink.io.nquads

import java.net.URI

import scala.reflect.ClassTag

import com.google.common.base.Predicates
import com.google.common.collect.Iterators
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.jena.atlas.io.PeekReader
import org.apache.jena.atlas.iterator.IteratorResourceClosing
import org.apache.jena.graph.Triple
import org.apache.jena.riot.SysRIOT.fmtMessage
import org.apache.jena.riot.lang.RiotParsers
import org.apache.jena.riot.system._
import org.apache.jena.riot.{RIOT, SysRIOT}
import org.apache.jena.sparql.core.Quad
import org.slf4j.{Logger, LoggerFactory}

import net.sansa_stack.rdf.benchmark.io.ReadableByteChannelFromIterator
import net.sansa_stack.rdf.common.io.riot.lang.LangNQuadsSkipBad
import net.sansa_stack.rdf.common.io.riot.tokens.TokenizerTextForgiving

import scala.collection.JavaConverters._
import org.apache.flink.streaming.api.scala._


/**
  * An N-Quads reader. One quad per line is assumed.
  *
  * @author Lorenz Buehmann
  */
object NQuadsReader {

  /**
    * Loads N-Quads data from a file or directory into a DataSet.
    *
    * @param env the Flink environment
    * @param path    the path to the N-Quads file(s)
    * @return the DataSet of triples
    */
  def load(env: ExecutionEnvironment, path: URI): DataSet[Triple] = {
    load(env, path.toString)
  }

  /**
    * Loads N-Quads data from a set of files or directories into a DataSet.
    * The path can also contain multiple paths
    * and even wildcards, e.g.
    * `"/my/dir1,/my/paths/part-00[0-5]*,/another/dir,/a/specific/file"`
    *
    * @param env the Flink environment
    * @param paths   the path to the N-Quads file(s)
    * @return the DataSet of triples
    */
  def load(env: ExecutionEnvironment, paths: Seq[URI]): DataSet[Triple] = {
    load(env, paths.mkString(","))
  }

  /**
    * Loads N-Quads data from a file or directory into a DataSet.
    * The path can also contain multiple paths
    * and even wildcards, e.g.
    * `"/my/dir1,/my/paths/part-00[0-5]*,/another/dir,/a/specific/file"`
    *
    * === Handling of errors===
    *
    * By default, it stops once a parse error occurs, i.e. a [[org.apache.jena.riot.RiotException]] will be thrown
    * generated by the underlying parser.
    *
    * The following options exist:
    *  - STOP the whole data loading process will be stopped and a `org.apache.jena.net.sansa_stack.rdf.spark.riot.RiotException` will be thrown
    *  - SKIP the line will be skipped but the data loading process will continue, an error message will be logged
    *
    *
    * ===Handling of warnings===
    *
    * If the additional checking of RDF terms is enabled, warnings during parsing can occur. For example,
    * a wrong lexical form of a literal w.r.t. to its datatype will lead to a warning.
    *
    * The following can be done with those warnings:
    *  - IGNORE the warning will just be logged to the configured logger
    *  - STOP similar to the error handling mode, the whole data loading process will be stopped and a
    *  [[org.apache.jena.riot.RiotException]] will be thrown
    *  - SKIP similar to the error handling mode, the line will be skipped but the data loading process will continue
    *
    *
    * ===Checking of RDF terms===
    * Set whether to perform checking of NTriples - defaults to no checking.
    *
    * Checking adds warnings over and above basic syntax errors.
    * This can also be used to turn warnings into exceptions if the option `stopOnWarnings` is set to STOP or SKIP.
    *
    *  - IRIs - whether IRIs confirm to all the rules of the IRI scheme
    *  - Literals: whether the lexical form conforms to the rules for the datatype.
    *  - Quads: check slots have a valid kind of RDF term (parsers usually make this a syntax error anyway).
    *
    *
    * See also the optional `errorLog` argument to control the output. The default is to log.
    *
    *
    * @param env        the Spark session
    * @param path           the path to the N-Quads file(s)
    * @param stopOnBadTerm  stop parsing on encountering a bad RDF term
    * @param stopOnWarnings stop parsing on encountering a warning
    * @param checkRDFTerms  run with checking of literals and IRIs either on or off
    * @param errorLog       the logger used for error message handling
    * @return the DataSet of triples
    */
  def load(env: ExecutionEnvironment,
           path: String,
           stopOnBadTerm: ErrorParseMode.Value = ErrorParseMode.STOP,
           stopOnWarnings: WarningParseMode.Value = WarningParseMode.IGNORE,
           checkRDFTerms: Boolean = false,
           errorLog: Logger = ErrorHandlerFactory.stdLogger)
  : DataSet[Triple] = {

    // parse the text file first
    val ds = env.readTextFile(path)

    val strict = stopOnBadTerm == ErrorParseMode.STOP && stopOnWarnings == WarningParseMode.STOP

    // create the error handler profile
    val profileWrapper = NonSerializableObjectWrapper {
      val errorHandler =
        if (strict) {
          ErrorHandlerFactory.errorHandlerStrict(errorLog)
        } else {
          if (stopOnBadTerm == ErrorParseMode.STOP) {
            if (stopOnWarnings == WarningParseMode.STOP || stopOnWarnings == WarningParseMode.SKIP) {
              ErrorHandlerFactory.errorHandlerStrict(errorLog)
            } else {
              ErrorHandlerFactory.errorHandlerStd(errorLog)
            }
          } else {
            //            ErrorHandlerFactory.errorHandlerWarn
            new CustomErrorHandler()
          }
        }

      new ParserProfileStd(RiotLib.factoryRDF, errorHandler, IRIResolver.create, PrefixMapFactory.createForInput, RIOT.getContext.copy, checkRDFTerms || strict, strict)
    }

    // parse each partition
    ds.mapPartition(p => {
      // convert iterator to input stream
      val input = ReadableByteChannelFromIterator.toInputStream(p.asJava)

      // create the parsing iterator
      val it =
        if (stopOnBadTerm == ErrorParseMode.STOP || stopOnWarnings == WarningParseMode.STOP) {
          // this is the default behaviour of Jena, i.e. once a parse error occurs the whole process stops
          RiotParsers.createIteratorNQuads(input, null, profileWrapper.get)
        } else {
          // here we "simply" skip illegal triples

          // we need a custom tokenizer
          val tokenizer = new TokenizerTextForgiving(PeekReader.makeUTF8(input))
          tokenizer.setErrorHandler(ErrorHandlerFactory.errorHandlerWarn)

          // which is used by a custom N-Triples iterator
          val it = new LangNQuadsSkipBad(tokenizer, profileWrapper.get, null)

          // filter out null values
          Iterators.filter(it, Predicates.notNull[Quad]())
        }
      p
      new IteratorResourceClosing[Quad](it, input).asScala
    })
      .map(_.asTriple())
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 0) println("Usage: NQuadsReader <PATH_TO_FILE>")

    val path = args(0)

    val env = ExecutionEnvironment.getExecutionEnvironment

    val ds = NQuadsReader.load(env,
      path,
      stopOnBadTerm = ErrorParseMode.SKIP,
      stopOnWarnings = WarningParseMode.SKIP,
      checkRDFTerms = true,
      LoggerFactory.getLogger("errorLog"))

    println(ds.count())

    println("result:\n" + ds.first(1000).map { _.toString.replaceAll("[\\x00-\\x1f]","???")}.collect().mkString("\n"))
  }

}

object ErrorParseMode extends Enumeration {
  val STOP, SKIP = Value
}

object WarningParseMode extends Enumeration {
  val STOP, SKIP, IGNORE = Value
}

private class NonSerializableObjectWrapper[T: ClassTag](constructor: => T) extends AnyRef with Serializable {
  @transient private lazy val instance: T = constructor

  def get: T = instance
}

private object NonSerializableObjectWrapper {
  def apply[T: ClassTag](constructor: => T): NonSerializableObjectWrapper[T] = new NonSerializableObjectWrapper[T](constructor)
}


/**
  * A custom error handler that doesn't throw an exception on fatal parse errors. This allows for simply skipping those
  * triples instead of aborting the whole parse process.
  *
  * @param log an optional logger
  */
class CustomErrorHandler(val log: Logger = SysRIOT.getLogger) extends ErrorHandler {

  /** report a warning */
  def logWarning(message: String, line: Long, col: Long): Unit = {
    if (log != null) log.warn(fmtMessage(message, line, col))
  }

  /** report an error */
  def logError(message: String, line: Long, col: Long): Unit = {
    if (log != null) log.error(fmtMessage(message, line, col))
  }

  /** report a catastrophic error */
  def logFatal(message: String, line: Long, col: Long): Unit = {
    if (log != null) logError(message, line, col)
  }

  override def warning(message: String, line: Long, col: Long): Unit = logWarning(message, line, col)

  override def error(message: String, line: Long, col: Long): Unit = logError(message, line, col)

  override def fatal(message: String, line: Long, col: Long): Unit = logFatal(message, line, col)
}

//sealed trait ErrorParseMode {
//  case object STOP extends ErrorParseMode
//  case object SKIP extends ErrorParseMode
//}
//sealed trait WarningParseMode {
//  case object STOP extends WarningParseMode
//  case object SKIP extends WarningParseMode
//  case object IGNORE extends WarningParseMode
//}
//@enum trait ErrorParseMode {
//  object STOP
//  object SKIP
//}