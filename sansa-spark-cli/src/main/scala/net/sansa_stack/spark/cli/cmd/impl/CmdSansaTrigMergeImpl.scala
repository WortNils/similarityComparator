package net.sansa_stack.spark.cli.cmd.impl

import com.google.common.base.Stopwatch
import net.sansa_stack.rdf.spark.model.rdd.RddOfDatasetOps
import net.sansa_stack.spark.cli.cmd.{CmdSansaTrigMerge, CmdSansaTrigQuery}
import org.aksw.commons.io.util.StdIo
import org.aksw.jena_sparql_api.rx.RDFLanguagesEx
import org.aksw.jena_sparql_api.utils.io.{StreamRDFDeferred, WriterStreamRDFBaseUtils}
import org.apache.jena.query.Dataset
import org.apache.jena.riot.system.{StreamRDFOps, StreamRDFWriter, SyntaxLabels}
import org.apache.jena.riot.writer.WriterStreamRDFBase
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.jena.shared.PrefixMapping
import org.apache.jena.shared.impl.PrefixMappingImpl
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import org.apache.commons.lang3.time.StopWatch


/**
 * Called from the Java class [[CmdSansaTrigMerge]]
 */
class CmdSansaTrigMergeImpl
object CmdSansaTrigMergeImpl {
  private val logger = LoggerFactory.getLogger(getClass)
  // JenaSystem.init()

  def run(cmd: CmdSansaTrigMerge): Integer = {

    import collection.JavaConverters._

    val stopwatch = StopWatch.createStarted()

    val prefixes: PrefixMapping = new PrefixMappingImpl()

    for (prefixSource <- cmd.outPrefixes.asScala) {
      logger.info("Adding prefixes from " + prefixSource)
      val tmp = RDFDataMgr.loadModel(prefixSource)
      prefixes.setNsPrefixes(tmp)
    }

    // val resultSetFormats = RDFLanguagesEx.getResultSetFormats
    // val outLang = RDFLanguagesEx.findLang(cmd.outFormat, resultSetFormats)

//    if (outLang == null) {
//      throw new IllegalArgumentException("No result set format found for " + cmd.outFormat)
//    }

//    logger.info("Detected registered result set format: " + outLang)


    val trigFiles = cmd.trigFiles.asScala
      .map(pathStr => Paths.get(pathStr).toAbsolutePath)
      .toList

    val validPaths = trigFiles
      .filter(Files.exists(_))
      .filter(!Files.isDirectory(_))
      .filter(Files.isReadable(_))
      .toSet

    val invalidPaths = trigFiles.toSet.diff(validPaths)
    if (!invalidPaths.isEmpty) {
      throw new IllegalArgumentException("The following paths are invalid (do not exist or are not a (readable) file): " + invalidPaths)
    }

    val spark = SparkSession.builder
      .master(cmd.sparkMaster)
      .appName(s"Trig Distinct ( $trigFiles )")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "1000") // MB
      .config("spark.kryo.registrator", String.join(
        ", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator"))
      .config("spark.sql.crossJoin.enabled", true)
      .getOrCreate()

    import net.sansa_stack.rdf.spark.io._

    val initialRdd: RDD[Dataset] = spark.sparkContext.union(
      validPaths
        .map(path => spark.datasets(Lang.TRIG)(path.toString)).toSeq)


    val effectiveRdd = RddOfDatasetOps.groupNamedGraphsByGraphIri(initialRdd, cmd.sort, cmd.distinct, cmd.numPartitions)

    val outRdfFormat = RDFLanguagesEx.findRdfFormat(cmd.outFormat)
      // RDFFormat.TRIG_BLOCKS

    if (cmd.outFolder == null && cmd.outFile == null) {

      val out = StdIo.openStdOutWithCloseShield

      // val out = Files.newOutputStream(Paths.get("output.trig"), StandardOpenOption.WRITE, StandardOpenOption.CREATE)
      // System.out
      val coreWriter = StreamRDFWriter.getWriterStream(out, outRdfFormat, null)

      if (coreWriter.isInstanceOf[WriterStreamRDFBase]) {
        WriterStreamRDFBaseUtils.setNodeToLabel(coreWriter.asInstanceOf[WriterStreamRDFBase], SyntaxLabels.createNodeToLabelAsGiven())
      }

      val writer = new StreamRDFDeferred(coreWriter, true,
        prefixes, cmd.deferOutputForUsedPrefixes, Long.MaxValue, null)

      writer.start
      StreamRDFOps.sendPrefixesToStream(prefixes, writer)

      // val it = effectiveRdd.collect
      val it = effectiveRdd.toLocalIterator
      for (dataset <- it) {
        StreamRDFOps.sendDatasetToStream(dataset.asDatasetGraph, writer)
      }
      writer.finish
      out.flush
    } else {
      if (cmd.outFile != null) {
        effectiveRdd.saveToFile(cmd.outFile, new PrefixMappingImpl(), outRdfFormat, cmd.outFolder)
      } else { // if (cmd.outFolder != null) {
        effectiveRdd.saveToFolder(cmd.outFolder, new PrefixMappingImpl(), outRdfFormat)
      }
    }

    // effectiveRdd.saveAsFile("outfile.trig.bz2", prefixes, RDFFormat.TRIG_BLOCKS)

    // effectiveRdd.coalesce(1)


    // ResultSetMgr.write(System.out, resultSetSpark.collectToTable().toResultSet, outLang)

    logger.info("Processing time: " + stopwatch.getTime(TimeUnit.SECONDS) + " seconds")

    0 // exit code
  }
}

