package net.sansa_stack.rdf.common.io.hadoop

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, SequenceInputStream}
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util
import java.util.concurrent.atomic.AtomicLong
import java.util.regex.{Matcher, Pattern}

import io.reactivex.Flowable
import io.reactivex.functions.Predicate
import org.aksw.jena_sparql_api.common.DefaultPrefixes
import org.aksw.jena_sparql_api.io.binseach._
import org.aksw.jena_sparql_api.rx.RDFDataMgrRx
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.jena.ext.com.google.common.primitives.Ints
import org.apache.jena.query.{Dataset, DatasetFactory}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat}

import scala.collection.JavaConverters._


/**
 * A record reader for Trig RDF files.
 *
 * @author Lorenz Buehmann
 * @author Claus Stadler
 */
class TrigRecordReader(val prefixMapping: Model = ModelFactory.createDefaultModel().setNsPrefixes(DefaultPrefixes.prefixes),
                       val maxRecordLength: Int = 200)
  extends RecordReader[LongWritable, Dataset] {

  private val trigFwdPattern: Pattern = Pattern.compile("@?base|@?prefix|(graph)?\\s*(<[^>]*>|_:[^-\\s]+)\\s*\\{", Pattern.CASE_INSENSITIVE)
  private val trigBwdPattern: Pattern = Pattern.compile("esab@?|xiferp@?|\\{\\s*(>[^<]*<|[^-\\s]+:_)\\s*(hparg)?", Pattern.CASE_INSENSITIVE)

  // private var start, end, position = 0L

  private val EMPTY_DATASET: Dataset = DatasetFactory.create

  private val currentKey = new AtomicLong
  private var currentValue: Dataset = DatasetFactory.create()

  private var datasetFlow: util.Iterator[Dataset] = _


  /**
    * Uses the matcher to find candidate probing positions, and returns the first positoin
    * where probing succeeds.
    * Matching ranges are part of the matcher configuration
    *
    * @param rawSeekable
    * @param m
    * @param isFwd
    * @param prober
    * @return
    */
  def findPosition(rawSeekable: Seekable, m: Matcher, isFwd: Boolean, prober: Seekable => Boolean): Long = {

    val seekable = rawSeekable.cloneObject
    val absMatcherStartPos = seekable.getPos

    while (m.find) {
      val start = m.start
      val end = m.end
      // The matcher yields absolute byte positions from the beginning of the byte sequence
      val matchPos = if (isFwd) {
        start
      } else {
        -end + 1
      }
      val absPos = (absMatcherStartPos + matchPos).asInstanceOf[Int]
      // Artificially create errors
      // absPos += 5;
      seekable.setPos(absPos)
      val probeSeek = seekable.cloneObject

      // val by = seekable.get()
      // println("by " + by)
      val probeResult = prober.apply(probeSeek)
      System.err.println(s"Probe result for matching at pos $absPos with fwd=$isFwd: $probeResult")

      if(probeResult) {
        return absPos
      }
    }

    -1L
  }


  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    val tmp = createDatasetFlowApproachEasyPeasy(inputSplit, context)

    datasetFlow = tmp.blockingIterable().iterator()
  }



  def createDatasetFlowApproachEasyPeasy(inputSplit: InputSplit, context: TaskAttemptContext): Flowable[Dataset] = {
//    val maxRecordLength = 200 // 10 * 1024
    val probeRecordCount = 1

    val split = inputSplit.asInstanceOf[FileSplit]
    val stream = split.getPath.getFileSystem(context.getConfiguration)
      .open(split.getPath)

    val splitStart = split.getStart
    val splitLength = split.getLength
    val splitEnd = splitStart + splitLength

    // Get a buffer for the split + 1 mrl for finding the first record in the next split + extra bytes to perform
    // record validation in the next split
    // But also don't step over a complete split
    val desiredBufferLength = splitLength + Math.min(maxRecordLength + probeRecordCount * maxRecordLength, splitLength - 1)
    val arr = new Array[Byte](Ints.checkedCast(desiredBufferLength))
    stream.skip(splitStart)
    val bufferLength = stream.read(arr, 0, arr.length)

    val extraLength = bufferLength - splitLength
    val dataRegionEnd = splitEnd + extraLength

    System.err.println("Processing split " + splitStart + " - " + splitEnd + " | --+" + extraLength + "--> " + dataRegionEnd)

    val baos = new ByteArrayOutputStream()
    RDFDataMgr.write(baos, prefixMapping, RDFFormat.TURTLE_PRETTY)
    val prefixBytes = baos.toByteArray


    // Clones the provided seekable!
    val effectiveInputStreamSupp: Seekable => InputStream = seekable => {
      val r = new SequenceInputStream(
        new ByteArrayInputStream(prefixBytes),
        Channels.newInputStream(seekable.cloneObject))
      r
    }

    val parser: Seekable => Flowable[Dataset] = seekable => {
      // TODO Close the cloned seekable
      val task = new java.util.concurrent.Callable[InputStream]() {
        def call(): InputStream = effectiveInputStreamSupp.apply(seekable)
      }

      val r = RDFDataMgrRx.createFlowableDatasets(task, Lang.TRIG, null)
      r
    }

    val isNonEmptyDataset = new Predicate[Dataset] {
      override def test(t: Dataset): Boolean = {
        // System.err.println("Dataset filter saw graphs: " + t.listNames().asScala.toList)
        !t.isEmpty
      }
    }

    val prober: Seekable => Boolean = seekable => {
      val quadCount = parser(seekable)
        .limit(probeRecordCount)
        .count
        .onErrorReturnItem(-1L)
        .blockingGet() > 0
      quadCount
    }

    val buffer = ByteBuffer.wrap(arr)
    val pageManager = new PageManagerForByteBuffer(buffer)
    val nav = new PageNavigator(pageManager)

    nav.setPos(0L)
    // Find the first record after the split end
    var firstNextRecordPos = splitEnd

    {
      // Set up absolute positions
      val absProbeRegionStart = splitEnd
      val absProbeRegionEnd = splitEnd + Math.min(maxRecordLength, extraLength) // = splitStart + bufferLength

      val relProbeRegionStart = 0L
      val relProbeRegionEnd = Ints.checkedCast(absProbeRegionEnd - absProbeRegionStart)

      // Data region is up to the end of the buffer
      val relDataRegionEnd = absProbeRegionStart + extraLength

      val seekable = nav.clone
      seekable.setPos(absProbeRegionStart - splitStart)
      seekable.limitNext(relDataRegionEnd)
      val charSequence = new CharSequenceFromSeekable(seekable)
      val fwdMatcher = trigFwdPattern.matcher(charSequence)
      fwdMatcher.region(0, relProbeRegionEnd)

      val matchPos = findPosition(seekable, fwdMatcher, true, prober)
      if(matchPos >= 0) {
        firstNextRecordPos = matchPos + splitStart
      }
    }


    var firstThisRecordPos = -1L;
    {
      // Set up absolute positions
      val absProbeRegionStart = splitStart
      val absProbeRegionEnd = splitStart + Math.min(maxRecordLength, splitLength) // = splitStart + bufferLength

      val relProbeRegionStart = 0L
      val relProbeRegionEnd = Ints.checkedCast(absProbeRegionEnd - absProbeRegionStart)

      // Data region is up to the end of the buffer
      val relDataRegionEnd = absProbeRegionStart + splitLength

      val seekable = nav.clone
      seekable.setPos(absProbeRegionStart - splitStart)
      seekable.limitNext(relDataRegionEnd)
      val charSequence = new CharSequenceFromSeekable(seekable)
      val fwdMatcher = trigFwdPattern.matcher(charSequence)
      fwdMatcher.region(0, relProbeRegionEnd)

      val matchPos = findPosition(seekable, fwdMatcher, true, prober)
      if(matchPos >= 0) {
        firstThisRecordPos = matchPos + splitStart
      }
    }

    var result: Flowable[Dataset] = null
    if(firstThisRecordPos >= 0) {
      val parseLength = firstNextRecordPos - firstThisRecordPos
      nav.setPos(firstThisRecordPos - splitStart)
      nav.limitNext(parseLength)
      result = parser(nav)
        //.onErrorReturnItem(EMPTY_DATASET)
        //.filter(isNonEmptyDataset)
    } else {
      result = Flowable.empty()
    }

    val cnt = result
      .count()
      .blockingGet()

    System.err.println("For effective region " + firstThisRecordPos + " - " + firstNextRecordPos + " got " + cnt + " datasets")

    result
  }




  def createDatasetFlowApproachComplex(inputSplit: InputSplit, context: TaskAttemptContext): Flowable[Dataset] = {
    val maxRecordLength = 200 // 10 * 1024
    val probeRecordCount = 1

    val twiceMaxRecordLengthMinusOne = 2 * maxRecordLength - 1

    // we have to prepend prefixes to help the parser as there is no other way to make it aware of those
    val baos = new ByteArrayOutputStream()
    RDFDataMgr.write(baos, prefixMapping, RDFFormat.TURTLE_PRETTY)
    val prefixBytes = baos.toByteArray

    // Clones the provided seekable!
    val effectiveInputStreamSupp: Seekable => InputStream = seekable => {
      val r = new SequenceInputStream(
        new ByteArrayInputStream(prefixBytes),
        Channels.newInputStream(seekable.cloneObject))
      r
    }

    val parser: Seekable => Flowable[Dataset] = seekable => {
      // TODO Close the cloned seekable
      val task = new java.util.concurrent.Callable[InputStream]() {
        def call(): InputStream = effectiveInputStreamSupp.apply(seekable)
      }

      val r = RDFDataMgrRx.createFlowableDatasets(task, Lang.TRIG, null)
      r
    }

    val isNonEmptyDataset = new Predicate[Dataset] {
      override def test(t: Dataset): Boolean = {
        // System.err.println("Dataset filter saw graphs: " + t.listNames().asScala.toList)
        !t.isEmpty
      }
    }

    val prober: Seekable => Boolean = seekable => {
      val quadCount = parser(seekable)
        .limit(probeRecordCount)
        .count
        .onErrorReturnItem(-1L)
        .blockingGet() > 0
      quadCount
    }

    // Check whether the prior split ended exactly at its boundary
    // For that, we check for a start position between [splitStart-2*mrl, splitStart-mrl]
    // If there is a position that can parse to the end, then all records of the prior
    // record will have been emitted
    // Otherwise, we search backwards from the offset of the current chunk
    // until we find a position from which probing over the chunk boundary works

    // split position in data (start one byte earlier to detect if
    // the split starts in the middle of a previous record)
    val split = inputSplit.asInstanceOf[FileSplit]
    // val start = 0L.max(split.getStart - 1)
    // val end = start + split.getLength
    val splitStart = split.getStart
    val splitLength = split.getLength
    val splitEnd = splitStart + splitLength

    // Block length is the maximum amound of data we need for processing of
    // an input split w.r.t. records crossing split boundaries
    // and extends over the start of the designated split region
    val blockStart = Math.max(splitStart - twiceMaxRecordLengthMinusOne, 0L)
    val blockLength = splitEnd - blockStart

    System.err.println("Processing split " + blockStart + " <--| " + splitStart + " - " + splitEnd)

    // open a stream to the data, pointing to the start of the split
    val stream = split.getPath.getFileSystem(context.getConfiguration)
      .open(split.getPath)


    // Note we could init the buffer from 0 to blocklength,
    // with 0 corresponding to blockstart
    // but then we would have to do more relative positioning
    val bufferSize = splitEnd // blockLength // inputSplit.getLength.toInt
    val arr = new Array[Byte](Ints.checkedCast(bufferSize))
    stream.readFully(0, arr)
    val buffer = ByteBuffer.wrap(arr)
    // buffer.position(split.getStart)
    // buffer.limit(split.getStart + split.getLength)

    val pageManager = new PageManagerForByteBuffer(buffer)
    val nav = new PageNavigator(pageManager)

    // Initially position at splitStart within the block (i.e. the byte buffer)
    val initNavPos = splitStart // splitStart - blockStart
    nav.setPos(initNavPos)


    // FIXME Creating splits has to ensure splits don't end on block/chunk boundaries
    // TODO Clarify terminology once more
 /*   if (blockStart == 0) {
      priorRecordEndsOnSplitBoundary = true
    }
*/

    // Find first valid offset in the current chunk - this serves as a boundary for scanning back
    var firstRecordPos = 0L

    // if (!priorRecordEndsOnSplitBoundary) {
    {
      // Set up absolute positions
      val absProbeRegionStart = splitStart // Math.min(splitStart - twiceMaxRecordLengthMinusOne, 0L)
      val absProbeRegionEnd = Math.min(absProbeRegionStart + maxRecordLength, splitEnd)
      // val absChunkEnd = splitStart
      val relProbeRegionStart = 0L // splitStart - absProbeRegionStart
      // Set up the matcher using relative positions
      val relProbeRegionEnd = Ints.checkedCast(absProbeRegionEnd - absProbeRegionStart)

      // Data region is up to the end of the split
      val relDataRegionEnd = splitEnd - absProbeRegionStart

      val seekable = nav.clone
      seekable.setPos(absProbeRegionStart)
      seekable.limitNext(relDataRegionEnd)
      val charSequence = new CharSequenceFromSeekable(seekable)
      val fwdMatcher = trigFwdPattern.matcher(charSequence)
      fwdMatcher.region(0, relProbeRegionEnd)

      firstRecordPos = findPosition(seekable, fwdMatcher, true, prober)
    }


    var continueLoop = firstRecordPos >= 0


    var effectiveFirstRecordPos = firstRecordPos

    // Now search backwards from the firstRecordPos
    // while the original previous boundary returns no records
    var absReparsePos = splitStart
    while(continueLoop) {
      // Reverse search until probing into our chunk succeeds

      // Note the values are 'backwards', i.e end < start
      val absProbeRegionStart = absReparsePos
      val absProbeRegionEnd = Math.max(splitStart - (maxRecordLength - 1), 0L)
      // val chunkEnd = splitEnd

      // Set up the matcher using relative positions
      val relProbeRegionEnd = Ints.checkedCast(absProbeRegionStart - absProbeRegionEnd)

      val relDataRegionEnd = splitEnd - firstRecordPos - 1 // (firstRecordPos - 1) - absProbeRegionStart
      val seekable = nav.clone
      seekable.setPos(absProbeRegionStart)
      seekable.limitNext(relDataRegionEnd)
      val reverseCharSequence = new ReverseCharSequenceFromSeekable(seekable.clone)
      val bwdMatcher = trigBwdPattern.matcher(reverseCharSequence)
      bwdMatcher.region(0, relProbeRegionEnd)

      val absProbeSuccessPos = findPosition(seekable, bwdMatcher, false, prober)

      if(absProbeSuccessPos >= 0) {
        // For the given probe position, make sure that these records could not be created with the original boundary

        val absValidateStart = absProbeSuccessPos
        val absValidateEnd = firstRecordPos
        val relValidateEnd = absValidateEnd - absValidateStart

        val originalPriorChunk = nav.clone
        seekable.setPos(absValidateStart)
        seekable.limitNext(relValidateEnd)

        val numValidateRecords = parser.apply(originalPriorChunk)
          .onErrorReturnItem(EMPTY_DATASET)
          .filter(isNonEmptyDataset)
          .count()
          .blockingGet()

        if (numValidateRecords != 0) {
          continueLoop = false
        } else {
          effectiveFirstRecordPos = absProbeSuccessPos
          absReparsePos = effectiveFirstRecordPos - 1
        }
      } else {
        continueLoop = false
        // absReparsePos = absProbeSuccessPos - 1
        // throw new RuntimeException("Should not happen")
      }
    }

    /*
    var probeSuccessPos = 0L
    if (!priorRecordEndsOnSplitBoundary) {
      // Set up absolute positions
      val absProbeRegionStart = blockStart // Math.min(splitStart - twiceMaxRecordLengthMinusOne, 0L)
      val absProbeRegionEnd = Math.max(splitStart - maxRecordLength, 0L)
      // val absChunkEnd = splitStart
      val relProbeRegionStart = splitStart - absProbeRegionStart
      if (relProbeRegionStart < twiceMaxRecordLengthMinusOne && absProbeRegionStart != 0) {
        System.err.println("WARNING: Insufficient preceding bytes before split start")
      }

      // Set up the matcher using relative positions
      val relProbeRegionEnd = Ints.checkedCast(absProbeRegionEnd - absProbeRegionStart)

      val seekable = nav.clone
      seekable.setPos(absProbeRegionStart)
      seekable.limitNext(relProbeRegionEnd)
      val charSequence = new CharSequenceFromSeekable(seekable)
      val fwdMatcher = trigFwdPattern.matcher(charSequence)
      fwdMatcher.region(0, relProbeRegionEnd)

      probeSuccessPos = findPosition(seekable, fwdMatcher, true, prober)

      priorRecordEndsOnSplitBoundary = probeSuccessPos >= 0
    }

    if (priorRecordEndsOnSplitBoundary) {
      /* we start at position 0 of the split */
      /* TODO We should still probe for an offset between
        0 and maxRecordLength for robustness
       */
      // TODO If the split is too small, we may not even get a single record
    }
    else {
      // Reverse search until probing into our chunk succeeds

      val absProbeRegionStart = splitStart
      val absProbeRegionEnd = Math.max(splitStart - (maxRecordLength - 1), 0L)
      // val chunkEnd = splitEnd

      // Set up the matcher using relative positions
      val relProbeRegionEnd = Ints.checkedCast(absProbeRegionStart - absProbeRegionEnd)

      val seekable = nav.clone
      seekable.setPos(absProbeRegionStart)
      seekable.limitNext(splitLength)
      val reverseCharSequence = new ReverseCharSequenceFromSeekable(seekable.clone)
      val bwdMatcher = trigBwdPattern.matcher(reverseCharSequence)
      bwdMatcher.region(0, relProbeRegionEnd)

      probeSuccessPos = findPosition(seekable, bwdMatcher, false, prober)

      if (probeSuccessPos < 0) {
        throw new RuntimeException("No suitable start found")
      }

      // Not sure whether or why we need +1 here
      // probeSuccessPos = probeSuccessPos + 1
      // nav.setPos(probeSuccessPos)
    }
*/
    var result: Flowable[Dataset] = null
    if(effectiveFirstRecordPos >= 0) {
      val parseLength = splitEnd - effectiveFirstRecordPos
      nav.setPos(effectiveFirstRecordPos)
      nav.limitNext(parseLength)
      result = parser(nav)
        .onErrorReturnItem(EMPTY_DATASET)
        .filter(isNonEmptyDataset)
    } else {
      result = Flowable.empty()
    }

      /*
      val str = IOUtils.toString(effectiveInputStreamSupp.apply(nav))
      System.err.println("Parser base data: "
        + str
        + "\nEND OF PARSER BASE DATA")
      */

    /*
    System.err.println("Parser base data 2: "
      + IOUtils.toString(effectiveInputStreamSupp.apply(nav))
      + "\nEND OF PARSER BASE DATA 2")

    if (probeSuccessPos > 0) {
      val ds = DatasetFactory.create
      RDFDataMgr.read(
        ds,
        new ByteArrayInputStream(str.getBytes()),
        Lang.TRIG)

      System.err.println("Got ds: " + ds.asDatasetGraph().size())
    }
    */

    val cnt = result
      .count()
      .blockingGet()

    System.err.println("Got " + cnt + " datasets")


    result
    /*
        // Lets start from this position
        nav.setPos(0)
        val absMatcherStartPos = nav.getPos

        // The charSequence has a clone of nav so it has independent relative positioning


        var matchCount = 0
        while (m.find && matchCount < 10) {
          val start = m.start
          val end = m.end
          // The matcher yields absolute byte positions from the beginning of the byte sequence
          val matchPos = if (isFwd) start else -end + 1
          val absPos = (absMatcherStartPos + matchPos).asInstanceOf[Int]
          // Artificially create errors
          // absPos += 5;
          nav.setPos(absPos)
          println(s"Attempting pos: $absPos")
          val navClone = nav.clone


          // if success, parse to Dataset
          if (quadCount >= 0) {
            matchCount += 1
            println(s"Candidate start pos $absPos yield $quadCount / $maxQuadCount quads")

            datasetFlow = RDFDataMgrRx.createFlowableDatasets(task, Lang.TRIG, null)
               // .doOnError(t => println(t))
              .blockingIterable()
              .iterator()

            return
          }
        }

    */
  }

  override def nextKeyValue(): Boolean = {
    if (datasetFlow == null || !datasetFlow.hasNext) {
      System.err.println("nextKeyValue: No more datasets")
      false
    }
    else {
      currentValue = datasetFlow.next()
      System.err.println("nextKeyValue: Got dataset value: " + currentValue.listNames().asScala.toList)
      RDFDataMgr.write(System.err, currentValue, RDFFormat.TRIG_PRETTY)
      System.err.println("nextKeyValue: Done getting dataset value")
      currentKey.incrementAndGet
      currentValue != null
    }
  }

  override def getCurrentKey: LongWritable = if (currentValue == null) null else new LongWritable(currentKey.get)

  override def getCurrentValue: Dataset = currentValue

  override def getProgress: Float = 0

  override def close(): Unit = {
    if (datasetFlow != null) {
      datasetFlow = null
    }
  }
}