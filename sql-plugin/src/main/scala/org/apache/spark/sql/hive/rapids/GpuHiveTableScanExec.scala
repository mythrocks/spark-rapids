/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.rapids

import ai.rapids.cudf.{HostMemoryBuffer, Schema, Table}

import com.nvidia.spark.rapids.GpuMetric.{BUFFER_TIME, DEBUG_LEVEL, DESCRIPTION_BUFFER_TIME, DESCRIPTION_FILTER_TIME, DESCRIPTION_GPU_DECODE_TIME, DESCRIPTION_PEAK_DEVICE_MEMORY, ESSENTIAL_LEVEL, FILTER_TIME, GPU_DECODE_TIME, MODERATE_LEVEL, NUM_OUTPUT_ROWS, PEAK_DEVICE_MEMORY}
import com.nvidia.spark.rapids.{CSVPartitionReader, ColumnarPartitionReaderWithPartitionValues, GpuColumnVector, GpuExec, GpuMetric, PartitionReaderIterator, PartitionReaderWithBytesRead, RapidsConf}
import com.nvidia.spark.rapids.shims.{ShimFilePartitionReaderFactory, ShimSparkPlan, SparkShimImpl}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.ql.metadata.{Partition => HivePartition}
import org.apache.hadoop.hive.ql.plan.TableDesc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.CastSupport
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeMap, AttributeReference, AttributeSet, BindReferences, Expression, Literal}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.{ExecSubqueryExpression, LeafExecNode, PartitionedFileUtil}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, DataType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

import java.net.URI
import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.JavaConverters._
import scala.collection.immutable.HashSet

case class GpuHiveTableScanExec(requestedAttributes: Seq[Attribute],
                                hiveTableRelation: HiveTableRelation,
                                partitionPruningPredicate: Seq[Expression])
  extends GpuExec with ShimSparkPlan with LeafExecNode with CastSupport {

  override def producedAttributes: AttributeSet = outputSet ++
    AttributeSet(partitionPruningPredicate.flatMap(_.references))

  private val originalAttributes = AttributeMap(hiveTableRelation.output.map(a => a -> a))

  override val output: Seq[Attribute] = {
    // Retrieve the original attributes based on expression ID so that capitalization matches.
    requestedAttributes.map(originalAttributes)
  }

  val partitionAttributes: Seq[AttributeReference] = hiveTableRelation.partitionCols

  // Bind all partition key attribute references in the partition pruning predicate for later
  // evaluation.
  private lazy val boundPruningPred = partitionPruningPredicate.reduceLeftOption(And).map { pred =>
    require(pred.dataType == BooleanType,
      s"Data type of predicate $pred must be ${BooleanType.catalogString} rather than " +
        s"${pred.dataType.catalogString}.")

    BindReferences.bindReference(pred, hiveTableRelation.partitionCols)
  }

  @transient private lazy val hiveQlTable = HiveClientImpl.toHiveTable(hiveTableRelation.tableMeta)
  @transient private lazy val tableDesc = new TableDesc(
    hiveQlTable.getInputFormatClass,
    hiveQlTable.getOutputFormatClass,
    hiveQlTable.getMetadata)

  private def castFromString(value: String, dataType: DataType) = {
    cast(Literal(value), dataType).eval(null)
  }

  /**
   * Prunes partitions not involve the query plan.
   *
   * @param partitions All partitions of the relation.
   * @return Partitions that are involved in the query plan.
   */
  private[hive] def prunePartitions(partitions: Seq[HivePartition]): Seq[HivePartition] = {
    boundPruningPred match {
      case None => partitions
      case Some(shouldKeep) => partitions.filter { part =>
        val dataTypes = hiveTableRelation.partitionCols.map(_.dataType)
        val castedValues = part.getValues.asScala.zip(dataTypes)
          .map { case (value, dataType) => castFromString(value, dataType) }

        // Only partitioned values are needed here, since the predicate has already been bound to
        // partition key attribute references.
        val row = InternalRow.fromSeq(castedValues)
        shouldKeep.eval(row).asInstanceOf[Boolean]
      }
    }
  }

   @transient lazy val prunedPartitions: Seq[HivePartition] = {
    if (hiveTableRelation.prunedPartitions.nonEmpty) {
      val hivePartitions =
        hiveTableRelation.prunedPartitions.get.map(HiveClientImpl.toHivePartition(_, hiveQlTable))
      if (partitionPruningPredicate.forall(!ExecSubqueryExpression.hasSubquery(_))) {
        hivePartitions
      } else {
        prunePartitions(hivePartitions)
      }
    } else {
      if (sparkSession.sessionState.conf.metastorePartitionPruning &&
        partitionPruningPredicate.nonEmpty) {
        rawPartitions
      } else {
        prunePartitions(rawPartitions)
      }
    }
  }

  // exposed for tests
  @transient lazy val rawPartitions: Seq[HivePartition] = {
    val prunedPartitions =
      if (sparkSession.sessionState.conf.metastorePartitionPruning &&
        partitionPruningPredicate.nonEmpty) {
        // Retrieve the original attributes based on expression ID so that capitalization matches.
        val normalizedFilters = partitionPruningPredicate.map(_.transform {
          case a: AttributeReference => originalAttributes(a)
        })
        sparkSession.sessionState.catalog
          .listPartitionsByFilter(hiveTableRelation.tableMeta.identifier, normalizedFilters)
      } else {
        sparkSession.sessionState.catalog.listPartitions(hiveTableRelation.tableMeta.identifier)
      }
    prunedPartitions.map(HiveClientImpl.toHivePartition(_, hiveQlTable))
  }

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    "numFiles" -> createMetric(ESSENTIAL_LEVEL, "number of files read"),
    "metadataTime" -> createTimingMetric(ESSENTIAL_LEVEL, "metadata time"),
    "filesSize" -> createSizeMetric(ESSENTIAL_LEVEL, "size of files read"),
    GPU_DECODE_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_GPU_DECODE_TIME),
    BUFFER_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_BUFFER_TIME),
    FILTER_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_FILTER_TIME),
    PEAK_DEVICE_MEMORY -> createSizeMetric(MODERATE_LEVEL, DESCRIPTION_PEAK_DEVICE_MEMORY),
    "scanTime" -> createTimingMetric(ESSENTIAL_LEVEL, "scan time")
  ) ++ semaphoreMetrics

  private def buildReader(sqlConf: SQLConf,
                          broadcastConf: Broadcast[SerializableConfiguration],
                          rapidsConf: RapidsConf,
                          dataSchema: StructType,
                          partitionSchema: StructType,
                          readSchema: StructType,
                          options: Map[String, String]
              ): PartitionedFile => Iterator[InternalRow] = {
    val readerFactory = GpuHiveTextPartitionReaderFactory(
      sqlConf = sqlConf,
      broadcastConf = broadcastConf,
      dataSchema = dataSchema,
      partitionSchema = partitionSchema,
      readSchema = readSchema,
      maxReaderBatchSizeRows = rapidsConf.maxReadBatchSizeRows,
      maxReaderBatchSizeBytes = rapidsConf.maxReadBatchSizeBytes,
      metrics = allMetrics,
      options = options
    )

    PartitionReaderIterator.buildReader(readerFactory)
  }

  private def getReadSchema(tableSchema: StructType,
                            partitionAttributes: Seq[Attribute],
                            requestedAttributes: Seq[Attribute]): StructType = {
    // TODO: Should this filter out partition fields? Handle later?
    val requestedSet: HashSet[String] = HashSet() ++ requestedAttributes.map(_.name)
    val partitionKeys: HashSet[String] = HashSet() ++ partitionAttributes.map(_.name)
    val prunedFields = tableSchema.filter {
      f => requestedSet.contains(f.name) && !partitionKeys.contains(f.name)
    }
    StructType(prunedFields)
  }

  private def createReadRDDForDirectories(
                readFile: PartitionedFile => Iterator[InternalRow],
                directories: Seq[(URI, InternalRow)], // TODO: Array[(URI, Seq[Any])],
                                                      // i.e. (path, partition_values).
                readSchema: StructType,
                sparkSession: SparkSession,
                hadoopConf: Configuration): RDD[InternalRow] = {

    def isNonEmptyDataFile(f: FileStatus): Boolean = {
      if (!f.isFile || f.getLen == 0) {
        false
      }
      else {
        val name = f.getPath.getName
        !((name.startsWith("_") && !name.contains("=")) || name.startsWith("."))
      }
    }

    val filePartitions: Seq[FilePartition] = directories.flatMap { case (directory, partValues) =>
      val path               = new Path(directory)
      val fs                 = path.getFileSystem(hadoopConf)
      val dirContents        = fs.listStatus(path).filter(isNonEmptyDataFile)
      val partitionDirectory = PartitionDirectory(partValues, dirContents)
      val maxSplitBytes      = FilePartition.maxSplitBytes(sparkSession, Array(partitionDirectory))

      val splitFiles: Seq[PartitionedFile] = dirContents.flatMap { f =>
        PartitionedFileUtil.splitFiles(
          sparkSession,
          f,
          f.getPath,
          isSplitable = true,
          maxSplitBytes,
          partitionDirectory.values
        )
      }.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
      FilePartition.getFilePartitions(sparkSession, splitFiles, maxSplitBytes)
    }

    // TODO: Handle small-file optimization. Currently assuming per-file reading.
    SparkShimImpl.getFileScanRDD(sparkSession, readFile, filePartitions, readSchema)
  }

  private def createReadRDDForTable(
                readFile: PartitionedFile => Iterator[InternalRow],
                hiveTableRelation: HiveTableRelation,
                readSchema: StructType,
                sparkSession: SparkSession,
                hadoopConf: Configuration
              ) = {
    val tableLocation: URI = hiveTableRelation.tableMeta.storage.locationUri.getOrElse{
      throw new UnsupportedOperationException("Table path not found")
    }

    createReadRDDForDirectories(readFile,
                                Array((tableLocation, InternalRow.empty)),
                                readSchema,
                                sparkSession,
                                hadoopConf)
  }

  private def createReadRDDForPartitions(
                readFile: PartitionedFile => Iterator[InternalRow],
                hiveTableRelation: HiveTableRelation,
                readSchema: StructType,
                sparkSession: SparkSession,
                hadoopConf: Configuration
              ): RDD[InternalRow] = {
    val partitionColTypes = hiveTableRelation.partitionCols.map(_.dataType)
    val dirsWithPartValues = prunedPartitions.map { p =>
      // TODO: Check for non-existence.
      val uri = p.getDataLocation.toUri
      val partValues: Seq[Any] = {
        p.getValues.asScala.zip(partitionColTypes).map {
          case (value, dataType) => castFromString(value, dataType)
        }
      }
      val partValuesAsInternalRow = InternalRow.fromSeq(partValues)

      (uri, partValuesAsInternalRow)
    }

    createReadRDDForDirectories(readFile,
      dirsWithPartValues, readSchema, sparkSession, hadoopConf)
//    throw new UnsupportedOperationException("Partitioned Hive text tables currently unsupported.")
  }

  lazy val inputRDD: RDD[InternalRow] = {
    // Assume Delimited text.
    // Note: The populated `options` aren't strictly required for text, currently.
    val options             = hiveTableRelation.tableMeta.properties ++
                              hiveTableRelation.tableMeta.storage.properties
    val hadoopConf          = sparkSession.sessionState.newHadoopConfWithOptions(options)
    val broadcastHadoopConf = sparkSession.sparkContext.broadcast(
                                new SerializableConfiguration(hadoopConf))
    val sqlConf             = sparkSession.sessionState.conf
    val rapidsConf          = new RapidsConf(sqlConf)
    val readSchema          = getReadSchema(hiveTableRelation.tableMeta.schema,
                                            partitionAttributes,
                                            requestedAttributes)

    val reader = buildReader(sqlConf,
                             broadcastHadoopConf,
                             rapidsConf,
                             hiveTableRelation.tableMeta.dataSchema,
                             hiveTableRelation.tableMeta.partitionSchema,
                             readSchema,
                             options)
    // TODO: sendDriverMetrics()
    if (hiveTableRelation.isPartitioned) {
      /*
      val partitionColTypes = hiveTableRelation.partitionCols.map(_.dataType)
      val dirsWithPartValues = prunedPartitions.map { p =>
        // TODO: Check for non-existence.
        val uri = p.getDataLocation.toUri
        val partValues: Seq[Any] = {
          p.getValues.asScala.zip(partitionColTypes).map {
            case(value, dataType) => castFromString(value, dataType)
          }
        }
        val partValuesAsInternalRow = InternalRow.fromSeq(partValues)

        (uri, partValuesAsInternalRow)
      }

      throw new UnsupportedOperationException("Partitioned Hive text tables currently unsupported.")

       */
      createReadRDDForPartitions(reader, hiveTableRelation, readSchema, sparkSession, hadoopConf)
    }
    else {
      createReadRDDForTable(reader, hiveTableRelation, readSchema, sparkSession, hadoopConf)
    }
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
//    if (hiveTableRelation.isPartitioned) {
//      throw new UnsupportedOperationException(
//        "Partitioned Hive text tables currently unsupported.")
//    }
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val scanTime = gpuLongMetric("scanTime")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].mapPartitionsInternal { batches =>
      new Iterator[ColumnarBatch] {

        override def hasNext: Boolean = {
          // The `FileScanRDD` returns an iterator which scans the file during the `hasNext` call.
          val startNs = System.nanoTime()
          val res = batches.hasNext
          scanTime += NANOSECONDS.toMillis(System.nanoTime() - startNs)
          res
        }

        override def next(): ColumnarBatch = {
          val batch = batches.next()
          numOutputRows += batch.numRows()
          batch
        }
      }
    }
  }
}

// Factory to build the columnar reader.
case class GpuHiveTextPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    partitionSchema: StructType,
    readSchema: StructType,
    maxReaderBatchSizeRows: Integer,
    maxReaderBatchSizeBytes: Long,
    metrics: Map[String, GpuMetric],
    @transient options: Map[String, String])
  extends ShimFilePartitionReaderFactory(options) {

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("Row-based text parsing is not supported on GPU.")
  }

  private val csvOptions = new CSVOptions(options,
                                          sqlConf.csvColumnPruning,
                                          sqlConf.sessionLocalTimeZone,
                                          sqlConf.columnNameOfCorruptRecord)

  override def buildColumnarReader(partFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastConf.value.value
    val reader = new PartitionReaderWithBytesRead(
                   new GpuHiveDelimitedTextPartitionReader(
                     conf, csvOptions, partFile, dataSchema,
                     readSchema, maxReaderBatchSizeRows,
                     maxReaderBatchSizeBytes, metrics))
    ColumnarPartitionReaderWithPartitionValues.newReader(partFile, reader, partitionSchema)
  }
}

// Reader that converts from chunked data buffers into cudf.Table.
class GpuHiveDelimitedTextPartitionReader(
    conf: Configuration,
    csvOptions: CSVOptions,
    partFile: PartitionedFile,
    dataSchema: StructType,
    readDataSchema: StructType,
    maxRowsPerChunk: Integer,
    maxBytesPerChunk: Long,
    execMetrics: Map[String, GpuMetric]) extends
  CSVPartitionReader(conf, partFile, dataSchema, readDataSchema,
                     csvOptions,
                     maxRowsPerChunk, maxBytesPerChunk, execMetrics) {

  override def buildCsvOptions(parsedOptions: CSVOptions,
                               schema: StructType,
                               hasHeader: Boolean): ai.rapids.cudf.CSVOptions.Builder = {
    super.buildCsvOptions(parsedOptions, schema, hasHeader)
      .withDelim('\u0001')
  }

  // TODO: Remove this. Only for debugging.
  override def readToTable(
      dataBuffer: HostMemoryBuffer,
      dataSize: Long,
      cudfSchema: Schema,
      readDataSchema: StructType,
      isFirstChunk: Boolean): Table = {
    val table = super.readToTable(dataBuffer, dataSize, cudfSchema, readDataSchema, isFirstChunk)
    GpuColumnVector.debug("Output table 2: ", table)
    table
  }
}
